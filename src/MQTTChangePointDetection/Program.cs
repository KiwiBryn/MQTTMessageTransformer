/*
Copyright(c) May 2026, devMobile Software

Change point detection identifies persistent distributional shifts — a lasting change in 
mean or variance — rather than transient spikes.

Output vector: [alert, rawScore, pValue, martingaleValue]
   martingaleValue accumulates evidence of change; higher = stronger signal.

Should be ready after 20-50 points, but can take 100+ for noisy data. Initial points are used to establish a baseline.

IID assumes each point is independent — no seasonality, no trends,
no sequence learning. It detects persistent shifts based purely on:
   statistical deviation from recent values
   distribution (mean / variance–like behaviour)

ChangeHistoryLength the only tuning knob
   Controls the martingale window: how many points after a shift the model
   keeps watching before resetting. Larger = slower to confirm, but fewer
   false positives on noisy data.

Scenario A — General use (recommended default)
   changeHistoryLength = 20
   Good balance
      Works well for most telemetry / IoT
      Responsive but not noisy

Scenario B — Noisy / high-variance data
   changeHistoryLength = 50 to 100
   Reduces false positives
      Smooths transient fluctuations
      Slower to confirm real shifts

Scenario C — Highly sensitive detection
   changeHistoryLength = 5 to 15
   Detects abrupt shifts quickly
      Reacts fast to changes
      More false positives
      May re-alert on the same shift

Scenario D — Small datasets / startup
   changeHistoryLength = 10 to 20
   Works with limited data
      Faster stabilization than SSA

Too many false change points
   Increase: changeHistoryLength -> 50 or 100

Missing change points
   Decrease: changeHistoryLength -> 10 to 15


SSA Is a univariate anomaly detection algorithm that uses Singular
   Spectrum Analysis to decompose the time series into components
   and identify persistent structural shifts based on the reconstruction error.

   It is effective for detecting change points in time series data with seasonality and trends.

   seasonalityWindowSize = expected seasonal period (pattern length)
   trainingWindowSize    = how much baseline history the model learns from
   changeHistoryLength   = martingale window / sensitivity vs stability

   Scenario A — No strong seasonality
      seasonalityWindowSize = 1 or 2
      trainingWindowSize = 200 to 500
      changeHistoryLength = 20

   Scenario B — Real periodic data (recommended)
   If you do have periodicity:
      Example: 60-point cycle (very common)
      changeHistoryLength = 20
      seasonalityWindowSize = 60
      trainingWindowSize = 600 to 1200

   Scenario C — Small datasets
   If data is limited:
      changeHistoryLength = 20
      seasonalityWindowSize = 10 to 20
      trainingWindowSize = 150 to 300

   Key constraints
      trainingWindowSize > seasonalityWindowSize
      trainingWindowSize > changeHistoryLength
      Seasonality guessed incorrectly -> change point detection fails

   Too many false change points:
      Increase:
      changeHistoryLength -> 50
      trainingWindowSize -> larger
   Miss change points:
      Decrease:
      changeHistoryLength -> 10 to 15
      trainingWindowSize -> slightly smaller

   Practical “safe default” — unsure, this works well in most cases:
      changeHistoryLength = 20
      seasonalityWindowSize = 60
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
   private static readonly ConcurrentDictionary<string, Lazy<TimeSeriesPredictionEngine<Model.TimeSeriesData, Model.ChangePointPrediction>>> _changePointEngines = new();

   // lock per topic because TimeSeriesPredictionEngine is not thread-safe
   private static readonly ConcurrentDictionary<string, object> _engineLocks = new();


   static async Task Main()
   {
      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Change point detection started");

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

               subscribedTopic.OutputMessageTransformer = CSScript.Evaluator.LoadFile<IChangePointOutputMessageTransformer>(subscribedTopic.OutputMessageTransformFile);
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

      var changePointEngine = _changePointEngines.GetOrAdd(subscribedTopic, _ =>
         new Lazy<TimeSeriesPredictionEngine<Model.TimeSeriesData, Model.ChangePointPrediction>>(() =>
         {
            try
            {
               switch (subscribedTopicSettings.DetectionMode)
               {
                  case Model.DetectionMode.IID:
                     var empty = _MLContext.Data.LoadFromEnumerable(new List<Model.TimeSeriesData>());

                     IidChangePointEstimator iidPipe = _MLContext.Transforms.DetectIidChangePoint(
                                    outputColumnName: nameof(Model.ChangePointPrediction.Prediction),
                                    inputColumnName: nameof(Model.TimeSeriesData.Value),
                                    confidence: subscribedTopicSettings.Confidence,
                                    changeHistoryLength: subscribedTopicSettings.ChangeHistoryLength);

                     var iidModel = iidPipe.Fit(empty);
                     var iidEngine = iidModel.CreateTimeSeriesEngine<Model.TimeSeriesData, Model.ChangePointPrediction>(_MLContext);

                     Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Initialized IID change point engine for '{subscribedTopic}' (pHistory:{subscribedTopicSettings.ChangeHistoryLength}, conf:{subscribedTopicSettings.Confidence})");
                     return iidEngine;

                  case Model.DetectionMode.SSA:
                     SsaChangePointEstimator ssaPipe = _MLContext.Transforms.DetectChangePointBySsa(
                                     outputColumnName: nameof(Model.ChangePointPrediction.Prediction),
                                     inputColumnName: nameof(Model.TimeSeriesData.Value),
                                     confidence: subscribedTopicSettings.Confidence,
                                     changeHistoryLength: subscribedTopicSettings.ChangeHistoryLength,
                                     trainingWindowSize: subscribedTopicSettings.TrainingWindowSize,
                                     seasonalityWindowSize: subscribedTopicSettings.SeasonalityWindowSize);

                     var dataView = _MLContext.Data.LoadFromEnumerable(new List<Model.TimeSeriesData>());
                     var ssaModel = ssaPipe.Fit(dataView);
                     var ssaEngine = ssaModel.CreateTimeSeriesEngine<Model.TimeSeriesData, Model.ChangePointPrediction>(_MLContext);

                     Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Initialized SSA change point engine for '{subscribedTopic}' (pHistory:{subscribedTopicSettings.ChangeHistoryLength}, conf:{subscribedTopicSettings.Confidence})");
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

      if (changePointEngine is null)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} No change point engine available for topic '{subscribedTopic}'");
         return;
      }

      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Change point prediction for value {value}");

      Model.ChangePointPrediction changePointPrediction;

      var engineLock = _engineLocks.GetOrAdd(subscribedTopic, _ => new object());

      lock (engineLock)
      {
         changePointPrediction = changePointEngine.Predict(new Model.TimeSeriesData { Value = value });
      }

      if (changePointPrediction.Prediction[0] == 1.0)
      {
         double rawScore = changePointPrediction.Prediction.Length > 1 ? changePointPrediction.Prediction[1] : double.NaN;
         double pValue = changePointPrediction.Prediction.Length > 2 ? changePointPrediction.Prediction[2] : double.NaN;
         double martingale = changePointPrediction.Prediction.Length > 3 ? changePointPrediction.Prediction[3] : double.NaN;

         byte[] payload;

         try
         {
            payload = subscribedTopicSettings.OutputMessageTransformer.Transform(subscribedTopic, value, rawScore, pValue, martingale);
         }
         catch (Exception ex)
         {
            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Change point detector output transform failed: {ex.Message}");
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
