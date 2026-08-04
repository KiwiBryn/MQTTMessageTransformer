/*
Copyright (c) May 2026, devMobile Software

*/
using CSScriptLib;
using devMobile.IoT.MqttTransformer.Detection.Model;
using devMobile.IoT.MqttTransformers;
using HiveMQtt.Client;
using HiveMQtt.Client.Events;
using HiveMQtt.MQTT5.ReasonCodes;
using HiveMQtt.MQTT5.Types;
using Microsoft.Extensions.Configuration;
using Microsoft.ML;
using System.Collections.Concurrent;

namespace devMobile.IoT.MqttTransformer.Detection;


class Program
{
   private static Model.ApplicationSettings _applicationSettings = default!;
   private static HiveMQClient? _client;

   // ML.NET context and per-topic state
   private static readonly MLContext _MLContext = new();

   // one engine per topic; keeps rolling state for IID detector
   private static readonly ConcurrentDictionary<string, Lazy<InferenceModel>> _lightGbmEngines = new();

   // lock per topic because _lightGbmEngines is not thread-safe
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

               switch (subscribedTopic.ModelType)
               {
                  case ModelType.Regression:
                     subscribedTopic.OutputMessageRegressionTransformer = CSScript.Evaluator.LoadFile<IOutputMessageRegressionTransformer>(subscribedTopic.OutputMessageTransformFile);
                     break;
                  case ModelType.BinaryClassification:
                     subscribedTopic.OutputMessageBinaryTransformer = CSScript.Evaluator.LoadFile<IOutputMessageBinaryTransformer>(subscribedTopic.OutputMessageTransformFile);

                     break;
                  case ModelType.MultiClassClassification:
                     subscribedTopic.OutputMessageClassificationTransformer = CSScript.Evaluator.LoadFile<IOutputMessageClassificationTransformer>(subscribedTopic.OutputMessageTransformFile);
                     break;
                  default:
                     throw new NotSupportedException();
               }
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

      switch (subscribedTopicSettings.ModelType)
      {
         case ModelType.Regression:
            if (subscribedTopicSettings.OutputMessageRegressionTransformer is null)
            {
               Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} No output regression transformer for topic: {subscribedTopic}");
               return;
            }
            break;
         case ModelType.BinaryClassification:
            if (subscribedTopicSettings.OutputMessageBinaryTransformer is null)
            {
               Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} No output binary transformer for topic: {subscribedTopic}");
               return;
            }
            break;
         case ModelType.MultiClassClassification:
            if (subscribedTopicSettings.OutputMessageClassificationTransformer is null)
            {
               Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} No output classification transformer for topic: {subscribedTopic}");
               return;
            }
            break;
         default:
            throw new NotSupportedException();
      }

      ModelInput modelInput = new ModelInput();
      try
      {
         modelInput.Features = subscribedTopicSettings.InputMessageTransformer.Transform(subscribedTopic, e.PublishMessage.Payload);
      }
      catch (Exception ex)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Input transform failed: {ex.Message}");
         return;
      }

      PredictionRegression predictionRegression = null!;
      PredictionBinary predictionBinary = null!;
      PredictionMultiClass predictionMultiClass = null!;

      InferenceModel inferenceModel = _lightGbmEngines.GetOrAdd(subscribedTopic, key => new Lazy<InferenceModel>(() =>
      {
         var settings = _applicationSettings.SubscribedTopics[key];
         using var fs = File.OpenRead(settings.ModelFileName);
         var model = _MLContext.Model.Load(fs, out _);

         return new InferenceModel
         {
            ModelType = settings.ModelType,
            RegressionEngine = settings.ModelType == ModelType.Regression ? _MLContext.Model.CreatePredictionEngine<ModelInput, PredictionRegression>(model) : null,
            BinaryEngine = settings.ModelType == ModelType.BinaryClassification ? _MLContext.Model.CreatePredictionEngine<ModelInput, PredictionBinary>(model) : null,
            MultiClassEngine = settings.ModelType == ModelType.MultiClassClassification ? _MLContext.Model.CreatePredictionEngine<ModelInput, PredictionMultiClass>(model) : null,
         };
      })).Value;

      lock (_engineLocks.GetOrAdd(subscribedTopic, _ => new object()))
      {
         switch (subscribedTopicSettings.ModelType)
         {
            case ModelType.Regression: predictionRegression = inferenceModel.RegressionEngine!.Predict(modelInput); break;
            case ModelType.BinaryClassification: predictionBinary = inferenceModel.BinaryEngine!.Predict(modelInput); break;
            case ModelType.MultiClassClassification: predictionMultiClass = inferenceModel.MultiClassEngine!.Predict(modelInput); break;
            default: throw new NotSupportedException();
         }
      }

      byte[] payload;

      try
      {
         switch (subscribedTopicSettings.ModelType)
         {
            case ModelType.Regression:
               payload = subscribedTopicSettings.OutputMessageRegressionTransformer!.Transform(subscribedTopic, predictionRegression);
               break;
            case ModelType.BinaryClassification:
               payload = subscribedTopicSettings.OutputMessageBinaryTransformer!.Transform(subscribedTopic, predictionBinary);
               break;
            case ModelType.MultiClassClassification:
               payload = subscribedTopicSettings.OutputMessageClassificationTransformer!.Transform(subscribedTopic, predictionMultiClass);
               break;
            default:
               throw new NotSupportedException();
         }

      }
      catch (Exception ex)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Output transform failed: {ex.Message}");
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

public class InferenceModel
{
   public required ModelType ModelType { get; init; }

   public PredictionEngine<ModelInput, PredictionRegression>? RegressionEngine { get; init; }

   public PredictionEngine<ModelInput, PredictionBinary>? BinaryEngine { get; init; }

   public PredictionEngine<ModelInput, PredictionMultiClass>? MultiClassEngine { get; init; }
}
