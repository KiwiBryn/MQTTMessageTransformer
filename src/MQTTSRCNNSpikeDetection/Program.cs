/*
Copyright (c) July 2026, devMobile Software

*/
using devMobile.IoT.MqttTransformers;

namespace devMobile.IoT.MqttTransformer.Detection;


class Program
{
   private static Model.ApplicationSettings _applicationSettings = default!;
   private static HiveMQClient? _client;

   private static MLContext _mlContext = new ();

   private static readonly ConcurrentDictionary<string, TopicRuntime> _TopicEstimators = new();

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
            optionsBuilder.WithClientCertificate(_applicationSettings.ClientCertificateFileName, _applicationSettings.ClientCertificatePassword);
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

            try
            {
               await Task.Delay(Timeout.Infinite, cts.Token);
            }
            catch (TaskCanceledException)
            {
               Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Application shutdown requested");
            }

            await _client.DisconnectAsync();
         }
      }
      catch (Exception ex)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Application startup failure {ex.Message}");
         Console.WriteLine(ex);
      }
      finally
      {
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

      var topicEstimator = _TopicEstimators.GetOrAdd(subscribedTopic, key => new TopicRuntime(_applicationSettings.SubscribedTopics[key]));

      List<Model.SpikePrediction> spikePredictions;
      lock (topicEstimator.Lock)
      {
         topicEstimator.Buffer.Enqueue(new Model.TimeSeriesData { Value = value });
         while (topicEstimator.Buffer.Count > topicEstimator.MaxBufferSize) topicEstimator.Buffer.Dequeue();

         if (topicEstimator.Buffer.Count < subscribedTopicSettings.SrCnnSettings.WindowSize)
         {
            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Not enough data for prediction (have {topicEstimator.Buffer.Count}, need {subscribedTopicSettings.SrCnnSettings.WindowSize})");
            return;
         }

         var data = _mlContext.Data.LoadFromEnumerable(topicEstimator.Buffer);

         if (topicEstimator.Transformer is null)
         {
            var estimator = _mlContext.Transforms.DetectAnomalyBySrCnn(
               outputColumnName: nameof(Model.SpikePrediction.Prediction),
               inputColumnName: nameof(Model.TimeSeriesData.Value),
               windowSize: subscribedTopicSettings.SrCnnSettings.WindowSize,
               backAddWindowSize: subscribedTopicSettings.SrCnnSettings.BackAddWindowSize,
               lookaheadWindowSize: subscribedTopicSettings.SrCnnSettings.LookaheadWindowSize,
               averagingWindowSize: subscribedTopicSettings.SrCnnSettings.AveragingWindowSize,
               judgementWindowSize: subscribedTopicSettings.SrCnnSettings.JudgementWindowSize,
               threshold: subscribedTopicSettings.SrCnnSettings.Threshold);
            topicEstimator.Transformer = estimator.Fit(data);
         }

         var transformed = topicEstimator.Transformer.Transform(data);
         spikePredictions = [.. _mlContext.Data.CreateEnumerable<Model.SpikePrediction>(transformed, reuseRowObject: false)];
      }

      var last = spikePredictions[^1].Prediction; // [alert, rawScore, mag]

      if (last[0] != 0f)
      {
         byte[] payload;
         try
         {
            payload = subscribedTopicSettings.OutputMessageTransformer.Transform(subscribedTopic, value, last[1], last[2]);
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
      }
      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.receive finish");
   }

   private sealed class TopicRuntime
   {
      public object Lock { get; } = new();
      public Queue<Model.TimeSeriesData> Buffer { get; }
      public int MaxBufferSize { get; }
      public ITransformer? Transformer { get; set; }

      public TopicRuntime(Model.TopicConfiguration configuration)
      {
         MaxBufferSize = configuration.SrCnnSettings.WindowSize+ configuration.SrCnnSettings.LookaheadWindowSize+ configuration.SrCnnSettings.BackAddWindowSize;

         Buffer = new Queue<Model.TimeSeriesData>(MaxBufferSize);
      }
   }
}
