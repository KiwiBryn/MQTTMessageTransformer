using devMobile.IoT.MqttTransformers;

namespace devMobile.IoT.MqttTransformer.Detection;


class Program
{
   private static Model.ApplicationSettings _applicationSettings = default!;
   private static HiveMQClient? _client;

   // ML.NET context and per-topic state
   private static readonly MLContext _MLContext = new MLContext();

   // one engine per topic; keeps rolling state for IID detector
   private static readonly ConcurrentDictionary<string, TimeSeriesPredictionEngine<Model.TimeSeriesData, Model.SpikePrediction>> _spikeEngines = new();

   // lock per topic because TimeSeriesPredictionEngine is not thread-safe
   private static readonly ConcurrentDictionary<string, object> _engineLocks = new();


   static async Task Main()
   { 
      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Hive MQ client starting");

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
             .WithUserName(_applicationSettings.UserName)
             .WithCleanStart(_applicationSettings.CleanStart)
             .WithUseTls(_applicationSettings.UseTls);

#if HIVEMQ_CERTIFICATE_SUPPORT
         if (!string.IsNullOrWhiteSpace(_applicationSettings.ClientCertificateFileName))
         {
            optionsBuilder.WithClientCertificate(_applicationSettings.ClientCertificateFileName,_applicationSettings.ClientCertificatePassword);
         }

         if (!string.IsNullOrWhiteSpace(_applicationSettings.Password))
         {
            optionsBuilder = optionsBuilder.WithPassword(_applicationSettings.Password);
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
         await _client.DisconnectAsync();

         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Hive MQ client stopped");
      }

      Console.WriteLine("Press Enter to exit");
      Console.ReadLine();
   }

   private static void OnMessageReceived(object? sender, OnMessageReceivedEventArgs e)
   {
      var client = (HiveMQClient)sender!;

      string subscribedTopic = e.PublishMessage.Topic ?? string.Empty;

      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.receive start Topic:{e.PublishMessage.Topic} QoS:{e.PublishMessage.QoS}");
            
      if (!_applicationSettings.SubscribedTopics.TryGetValue(subscribedTopic, out var subscribedTopicSettings))
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} no topic match:{subscribedTopic}");
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
      {
         var empty = _MLContext.Data.LoadFromEnumerable(new List<Model.TimeSeriesData>());
         var pipe = _MLContext.Transforms.DetectIidSpike(
                         outputColumnName: nameof(Model.SpikePrediction.Prediction),
                         inputColumnName: nameof(Model.TimeSeriesData.Value),
                         confidence: subscribedTopicSettings.Confidence,
                         pvalueHistoryLength: subscribedTopicSettings.PValueHistoryLength);

         var model = pipe.Fit(empty);

         var engine = model.CreateTimeSeriesEngine<Model.TimeSeriesData, Model.SpikePrediction>(_MLContext);

         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Initialized IID spike engine for '{e.PublishMessage.Topic}' (pHistory:{subscribedTopicSettings.PValueHistoryLength}, conf:{subscribedTopicSettings.Confidence})");
         return engine;
      });

      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Spike prediction for value {value}");

      Model.SpikePrediction spikePrediction;

      var engineLock = _engineLocks.GetOrAdd(e.PublishMessage.Topic, _ => new object());

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
            payload = subscribedTopicSettings.OutputMessageTransformer.Transform(e.PublishMessage.Topic, rawScore, pValue);
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
               var resultPublish = client.PublishAsync(message).Result;
   
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
