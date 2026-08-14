/*
Copyright (c) May 2026, devMobile Software

*/
using CSScriptLib;
using devMobile.IoT.MqttTransFormer.LightGBM.Model;
using HiveMQtt.Client;
using HiveMQtt.Client.Events;
using HiveMQtt.MQTT5.ReasonCodes;
using HiveMQtt.MQTT5.Types;

namespace devMobile.IoT.MqttTransFormer.LightGBM;


class Program
{
   private static Model.ApplicationSettings _applicationSettings = default!;
   private static HiveMQClient? _client;

   // ML.NET context and per-topic state
   private static readonly MLContext _MLContext = new();

   // One predictor per topic, built eagerly at startup (all-or-nothing).
   private static readonly ConcurrentDictionary<string, ITopicPredictor> _predictors = new();

   static async Task Main()
   {
      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} LightGBM detection started");

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
            optionsBuilder.WithClientCertificate(_applicationSettings.ClientCertificateFileName, _applicationSettings.ClientCertificatePassword);
         }
#endif

         // Eagerly load transformer scripts + prediction engines per topic.
         // All-or-nothing: any failure aborts startup before we subscribe.
         try
         {
            foreach (var (topicName, cfg) in _applicationSettings.SubscribedTopics)
            {
               cfg.InputMessageTransformer = CSScript.Evaluator
                  .LoadFile<IInputMessageTransformer>(cfg.InputMessageTransformFile);

               switch (cfg.ModelType)
               {
                  case ModelType.Regression:
                     cfg.OutputMessageRegressionTransformer = CSScript.Evaluator
                        .LoadFile<IOutputMessageRegressionTransformer>(cfg.OutputMessageTransformFile);
                     break;
                  case ModelType.BinaryClassification:
                     cfg.OutputMessageBinaryTransformer = CSScript.Evaluator
                        .LoadFile<IOutputMessageBinaryTransformer>(cfg.OutputMessageTransformFile);
                     break;
                  case ModelType.MultiClassClassification:
                     cfg.OutputMessageClassificationTransformer = CSScript.Evaluator
                        .LoadFile<IOutputMessageClassificationTransformer>(cfg.OutputMessageTransformFile);
                     break;
                  default:
                     throw new NotSupportedException($"ModelType '{cfg.ModelType}' not supported for topic '{topicName}'.");
               }

               _predictors[topicName] = PredictorFactory.Create(_MLContext, cfg);

               Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss}   Loaded topic '{topicName}' " +
                                 $"({cfg.ModelType}, model='{Path.GetFileName(cfg.ModelFileName)}')");
            }
         }
         catch (Exception ex)
         {
            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Failed to load topics: {ex.Message}");
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

      if (!_applicationSettings.SubscribedTopics.TryGetValue(subscribedTopic, out var subscribedTopicSettings) ||
          !_predictors.TryGetValue(subscribedTopic, out var predictor))
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} no topic match:{subscribedTopic}");
         return;
      }

      // 1. Input transform (bytes → feature vector)
      ModelInput modelInput = new ModelInput();
      try
      {
         modelInput.Features = subscribedTopicSettings.InputMessageTransformer!
            .Transform(subscribedTopic, e.PublishMessage.Payload);
      }
      catch (Exception ex)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Input transform failed: {ex.Message}");
         return;
      }

      if (subscribedTopicSettings.ShowFeatureValues)
      {
         foreach (var feature in modelInput.Features!)
         {
            Console.Write($"{feature} ");
         }
         Console.WriteLine();
      }

      // 2. Predict + output transform (single call, strategy-owned lock)
      byte[] payload;
      try
      {
         payload = predictor.PredictAndTransform(subscribedTopic, modelInput);
      }
      catch (Exception ex)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Predict/output transform failed: {ex.Message}");
         return;
      }

      // 3. Publish
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