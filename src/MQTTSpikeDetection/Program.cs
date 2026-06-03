/*
Copyright (c) May 2026, devMobile Software

IID assumes each point is independent — no seasonality, no trends, 
no sequence learning. It detects anomalies based purely on:
   statistical deviation from recent values
   distribution (mean / variance–like behavior

   pvalueHistoryLength the only tuning knob
   Number of recent points used to estimate distribution which 
   controls sensitivity vs stability

Scenario A — General use (recommended default)
   pvalueHistoryLength = 32
   Good balance
      Works well for most telemetry / IoT
      Responsive but not noisy

Scenario B — Noisy / high-variance data
   pvalueHistoryLength = 64 to 128
   Reduces false positives
      Smooths random spikes
      Slower to detect real anomalies

Scenario C — Highly sensitive detection
   pvalueHistoryLength = 8 to 24
   Detects sudden spikes quickly
      Reacts fast to changes
      More false positives
      More jitter

Scenario D — Small datasets / startup
   pvalueHistoryLength = 16 to 32
   Works with limited data
      Faster stabilization than SSA

Too many false spikes
   Increase: pvalueHistoryLength -> 64 or 128

Missing spikes
   Decrease: pvalueHistoryLength -> 16 to 24

SSA Is a univariate anomaly detection algorithm that uses Singular 
   Spectrum Analysis to decompose the time series into components 
   and identify anomalies based on the reconstruction error. 

   It is effective for detecting anomalies in time series data with seasonality and trends.

   seasonalWindowSize = pattern length
   trainingWindowSize = how much history the model learns from
   pvalueHistoryLength = sensitivity / stability

   Scenario A — No strong seasonality
      seasonalWindowSize = 1 or 2
      trainingWindowSize = 200 to 500

   Scenario B — Example with real periodic data (recommended)
   If you do have periodicity:
      Example: 60-point cycle (very common)
      pvalueHistoryLength = 32
      seasonalWindowSize = 60
      trainingWindowSize = 600 to 1200

   Scenario C — Small datasets
   If data is limited:
      pvalueHistoryLength = 32
      seasonalWindowSize = 10 to 20
      trainingWindowSize = 150 to 300

   Key constraints
      trainingWindowSize > seasonalWindowSize
      trainingWindowSize > pvalueHistoryLength
      Seasonality guessed incorrectly -> spike detection fails

   Too many false spikes:
      Increase:
      pvalueHistoryLength -> 64
      trainingWindowSize -> larger
   Miss spikes:
      Decrease:
      pvalueHistoryLength -> 16 to 24
      trainingWindowSize -> slightly smaller

   Practical “safe default” = unsure, this works well in most cases:
      pvalueHistoryLength = 32
      seasonalWindowSize = 60
      trainingWindowSize = 800
*/
using devMobile.IoT.MqttTransformers;

namespace devMobile.IoT.MqttTransformer.Detection;


class Program
{
   private static Model.ApplicationSettings _applicationSettings = default!;
   private static HiveMQClient? _client;

   // ML.NET context and per-topic state
   private static readonly MLContext _MLContext = new();

   // one engine per topic; keeps rolling state for IID detector
   private static readonly ConcurrentDictionary<string, Lazy<TimeSeriesPredictionEngine<Model.TimeSeriesData, Model.SpikePrediction>>> _spikeEngines = new();

   // lock per topic because TimeSeriesPredictionEngine is not thread-safe
   private static readonly ConcurrentDictionary<string, object> _engineLocks = new();


   static async Task Main()
   {
      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Spike detection started");

      try
      {
         // Load configuration
         var configuration = new ConfigurationBuilder()
           .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
           .AddUserSecrets<Program>()
           .Build();

         _applicationSettings = configuration.GetSection("ApplicationSettings").Get<Model.ApplicationSettings>() ?? throw new Exception("ApplicationSettings not configured");

         // HiveMQ client options
         var optionsBuilder = new HiveMQClientOptionsBuilder()
            .WithClientId(_applicationSettings.ClientId)
            .WithBroker(_applicationSettings.Host)
            .WithPort(_applicationSettings.Port)
#if HIVEMQ_CERTIFICATE_SUPPORT
            .WithClientCertificate(_applicationSettings.ClientCertificateFileName, _applicationSettings.ClientCertificatePassword);
#endif
#if HIVEMQ_USERNAME_AND_PASSWORD_SUPPORT
            .WithUserName(_applicationSettings.UserName)
            .WithPassword(_applicationSettings.Password)
#endif
            .WithCleanStart(_applicationSettings.CleanStart)
            .WithAutomaticReconnect(_applicationSettings.AutomaticReconnect)
            .WithUseTls(_applicationSettings.UseTls);

#if HIVEMQ_CERTIFICATE_SUPPORT
         if (!string.IsNullOrWhiteSpace(_applicationSettings.ClientCertificateFileName))
         {
            optionsBuilder.WithClientCertificate(_applicationSettings.ClientCertificateFileName,_applicationSettings.ClientCertificatePassword);
         }
#endif

         try
         {
            foreach (var subscribedTopic in _applicationSettings.SubscribedTopics.Values)
            {
               subscribedTopic.InputMessageTransformer = CSScript.Evaluator.LoadFile<IInputMessageTransformer>(subscribedTopic.InputMessageTransformFile);

               subscribedTopic.OutputMessageTransformer = CSScript.Evaluator.LoadFile<ISpikeOutputMessageTransformer>(subscribedTopic.OutputMessageTransformFile);
            }
         }
         catch (Exception ex)
         {
            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Failed to load message transformer scripts: {ex.Message}");
            throw;
         }

         var cts = new CancellationTokenSource();
         Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

         using (_client = new HiveMQClient(optionsBuilder.Build()))
         {
            _client.OnMessageReceived += OnMessageReceived;

            var connectResult = await _client.ConnectAsync();
            if (connectResult.ReasonCode != ConnAckReasonCode.Success)
            {
               throw new Exception($"Failed to connect: {connectResult.ReasonCode}");
            }

            Console.WriteLine("Subscribing to Topic(s)");
            foreach (var subscribedTopic in _applicationSettings.SubscribedTopics)
            {
               var subscribedTopicValue = subscribedTopic.Value;

               var subscribeResult = await _client.SubscribeAsync(subscribedTopic.Key, subscribedTopicValue.InputQualityOfService);

               Console.WriteLine($"  Topic:{subscribedTopic.Key} Result:{subscribeResult.Subscriptions[0].SubscribeReasonCode}");
            }

            await Task.Delay(Timeout.Infinite, cts.Token);
         }
      }
      catch (TaskCanceledException)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Application shutdown requested");
      }
      catch (Exception ex)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Application startup failure {ex.Message}");
         Console.WriteLine(ex);
      }
      finally
      {
         if (_client is not null)
         {
            await _client.DisconnectAsync();
         }

         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Hive MQ client stopped");
      }

      Console.WriteLine("Press Enter to exit");
      Console.ReadLine();
   }

   private static async void OnMessageReceived(object? sender, OnMessageReceivedEventArgs e)
   {
      try
      {
         await OnMessageReceivedCoreAsync(sender, e);
      }
      catch (Exception ex)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Unhandled exception in message handler: {ex.Message}");
      }
   }

   private static async Task OnMessageReceivedCoreAsync(object? sender, OnMessageReceivedEventArgs e)
   {
      var client = (HiveMQClient)sender!;

      string subscribedTopic = e.PublishMessage.Topic ?? string.Empty;
      if (string.IsNullOrEmpty(subscribedTopic))
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} No topic for received message");
         return;
      }
      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.receive start Topic:{subscribedTopic} QoS:{e.PublishMessage.QoS}");

      if (e.PublishMessage.Payload is null)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} No payload for received message on topic {subscribedTopic}");
         return;
      }

      if (!_applicationSettings.SubscribedTopics.TryGetValue(subscribedTopic, out var subscribedTopicSettings))
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} no topic match:{subscribedTopic}");
         return;
      }

      if (subscribedTopicSettings.InputMessageTransformer is null)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} No input transformer for topic: {subscribedTopic}");
         return;
      }

      if (subscribedTopicSettings.OutputMessageTransformer is null)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} No output transformer for topic: {subscribedTopic}");
         return;
      }

      float value;
      try
      {
         value = subscribedTopicSettings.InputMessageTransformer.Transform(subscribedTopic, e.PublishMessage.Payload);
      }
      catch (Exception ex)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Input transform failed: {ex.Message}");
         return;
      }

      var spikeEngine = _spikeEngines.GetOrAdd(subscribedTopic, _ =>
         new Lazy<TimeSeriesPredictionEngine<Model.TimeSeriesData, Model.SpikePrediction>>(() =>
         {
            try
            {
               switch (subscribedTopicSettings.DetectionMode)
               {
                  case Model.DetectionMode.IID:
                     var empty = _MLContext.Data.LoadFromEnumerable(new List<Model.TimeSeriesData>());

                     IidSpikeEstimator iidPipe = _MLContext.Transforms.DetectIidSpike(
                                    outputColumnName: nameof(Model.SpikePrediction.Prediction),
                                    inputColumnName: nameof(Model.TimeSeriesData.Value),
                                    confidence: subscribedTopicSettings.Confidence,
                                    pvalueHistoryLength: subscribedTopicSettings.PValueHistoryLength);

                     var iidModel = iidPipe.Fit(empty);
                     var iidEngine = iidModel.CreateTimeSeriesEngine<Model.TimeSeriesData, Model.SpikePrediction>(_MLContext);

                     Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Initialized IID spike engine for '{subscribedTopic}' (pHistory:{subscribedTopicSettings.PValueHistoryLength}, conf:{subscribedTopicSettings.Confidence})");
                     return iidEngine;

                  case Model.DetectionMode.SSA:
                     SsaSpikeEstimator ssaPipe = _MLContext.Transforms.DetectSpikeBySsa(
                                     outputColumnName: nameof(Model.SpikePrediction.Prediction),
                                     inputColumnName: nameof(Model.TimeSeriesData.Value),
                                     confidence: subscribedTopicSettings.Confidence,
                                     pvalueHistoryLength: subscribedTopicSettings.PValueHistoryLength,
                                     trainingWindowSize: subscribedTopicSettings.TrainingWindowSize,
                                     seasonalityWindowSize: subscribedTopicSettings.SeasonalityWindowSize);

                     var dataView = _MLContext.Data.LoadFromEnumerable(new List<Model.TimeSeriesData>());
                     var ssaModel = ssaPipe.Fit(dataView);
                     var ssaEngine = ssaModel.CreateTimeSeriesEngine<Model.TimeSeriesData, Model.SpikePrediction>(_MLContext);

                     Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Initialized SSA spike engine for '{subscribedTopic}' (pHistory:{subscribedTopicSettings.PValueHistoryLength}, conf:{subscribedTopicSettings.Confidence})");
                     return ssaEngine;
                  default:
                     throw new NotSupportedException($"Detection mode {subscribedTopicSettings.DetectionMode} is not supported.");
               }
            }
            catch (Exception ex)
            {
               Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Failed to initialize change point engine for topic '{subscribedTopic}': {ex.Message}");
               return null!;
            }
         }, LazyThreadSafetyMode.PublicationOnly)).Value;

      if (spikeEngine is null)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} No spike engine available for topic '{subscribedTopic}'");
         return;
      }  

      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Spike prediction for value {value}");

      Model.SpikePrediction spikePrediction;

      var engineLock = _engineLocks.GetOrAdd(subscribedTopic, _ => new object());

      lock (engineLock)
      {
         spikePrediction = spikeEngine.Predict(new Model.TimeSeriesData { Value = value });
      }

      if (spikePrediction.Prediction[0] == 1.0)
      {
         double rawScore = spikePrediction.Prediction.Length > 1 ? spikePrediction.Prediction[1] : double.NaN;
         double pValue = spikePrediction.Prediction.Length > 2 ? spikePrediction.Prediction[2] : double.NaN;

         byte[] payload;

         try
         {
            payload = subscribedTopicSettings.OutputMessageTransformer.Transform(subscribedTopic, value, rawScore, pValue);
         }
         catch (Exception ex)
         {
            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Spike detector output transform failed: {ex.Message}");
            return;
         }

         string[] topics = subscribedTopicSettings.OutputTopic.Split(',', StringSplitOptions.RemoveEmptyEntries);

         foreach (string topic in topics)
         {
            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Publishing to:{topic}");

            var message = new MQTT5PublishMessage(topic, subscribedTopicSettings.OutputQualityOfService)
            {
               ContentType = subscribedTopicSettings.ContentType,
               Payload = payload
            };

            try
            {
               var resultPublish = await client.PublishAsync(message);

               Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Published:{resultPublish.QoS1ReasonCode} {resultPublish.QoS2ReasonCode}");
            }
            catch (Exception ex)
            {
               Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} client.PublishAsync failed: {ex.Message}");
               return;
            }
         }

         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.receive finish");
      }
   }
}
