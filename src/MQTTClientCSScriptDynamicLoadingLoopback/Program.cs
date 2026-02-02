//---------------------------------------------------------------------------------
// Copyright (c) February 2026, devMobile Software
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// https://github.com/hivemq/hivemq-mqtt-client-dotnet 
//
using HiveMQtt.Client;
using HiveMQtt.MQTT5.ReasonCodes;
using HiveMQtt.MQTT5.Types;

namespace devMobile.IoT.MqttTransformer.CSScriptDynamicLoadingLoopback;

class Program
{
   private static Model.ApplicationSettings _applicationSettings;
   private static readonly MemoryCache _cache = new MemoryCache(new MemoryCacheOptions());
   //private static readonly ScriptEngine _scriptEngine = new ScriptEngine(_cache, "transforms\\TransFormerMissing.cs");
   //private static readonly ScriptEngine _scriptEngine = new ScriptEngine(_cache, "transforms\\broken.cs");
   //private static readonly ScriptEngine _scriptEngine = new ScriptEngine(_cache, "transforms\\lower.cs");
   //private static readonly ScriptEngine _scriptEngine = new ScriptEngine(_cache, "transforms\\upper.cs");
   //private static readonly ScriptEngine _scriptEngine = new ScriptEngine(_cache, "transforms\\lowerUpper.cs");
   //private static readonly ScriptEngine _scriptEngine = new ScriptEngine(_cache, "transforms\\returnsNullmessage.cs");
   //private static readonly ScriptEngine _scriptEngine = new ScriptEngine(_cache, "transforms\\returnsOneNullmessageEnd.cs");
   //private static readonly ScriptEngine _scriptEngine = new ScriptEngine(_cache, "transforms\\returnsOneNullmessageMiddle.cs");
   //private static readonly ScriptEngine _scriptEngine = new ScriptEngine(_cache, "transforms\\returnsOneNullmessageStart.cs");
   private static readonly ScriptEngine _scriptEngine = new ScriptEngine(_cache, "transforms\\BrokenTopic.cs");


   static async Task Main()
   {
      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Hive MQ client coode lookback starting");

      try
      {
         // load the app settings into configuration
         var configuration = new ConfigurationBuilder()
              .AddJsonFile("appsettings.json", false, true)
              .AddUserSecrets<Program>()
         .Build();

         _applicationSettings = configuration.GetSection("ApplicationSettings").Get<Model.ApplicationSettings>();
         if (_applicationSettings is null)
         {
            throw new Exception("ApplicationSettings not configured");
         }
     
         var optionsBuilder = new HiveMQClientOptionsBuilder();

         optionsBuilder
            .WithClientId(_applicationSettings.ClientId)
            .WithBroker(_applicationSettings.Host)
            .WithPort(_applicationSettings.Port)
            .WithUserName(_applicationSettings.UserName)
            .WithCleanStart(_applicationSettings.CleanStart)
            .WithUseTls(_applicationSettings.UseTls);

         if (!string.IsNullOrWhiteSpace(_applicationSettings.ClientCertificateFileName))
         {
            optionsBuilder.WithClientCertificate(_applicationSettings.ClientCertificateFileName, _applicationSettings.ClientCertificatePassword);
         }

         if (!string.IsNullOrWhiteSpace(_applicationSettings.Password))
         {
            optionsBuilder = optionsBuilder.WithPassword(_applicationSettings.Password);
         }

         using (HiveMQClient _client = new HiveMQClient(optionsBuilder.Build()))
         {
            _client.OnMessageReceived += OnMessageReceived;

            var connectResult = await _client.ConnectAsync();
            if (connectResult.ReasonCode != ConnAckReasonCode.Success)
            {
               throw new Exception($"Failed to connect: {connectResult.ReasonCode}");
            }

            Console.WriteLine($"Subscribing to Topic(s)");
            foreach (string topic in _applicationSettings.SubscribeTopics.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
               string topicFormatted = string.Format(topic, _applicationSettings.ClientId);

               SubscribeOptionsBuilder subscribeOptionsBuilder = new SubscribeOptionsBuilder();
               TopicFilter[] topicFilter = [new TopicFilter(topicFormatted, _applicationSettings.SubscribeQualityOfService)];
               subscribeOptionsBuilder.WithSubscriptions(topicFilter);
               var subscribeResult = await _client.SubscribeAsync(topicFormatted, _applicationSettings.SubscribeQualityOfService);

               Console.WriteLine($" Topic:{topicFormatted} Result:{subscribeResult.Subscriptions[0].SubscribeReasonCode}");
            }

            try
            {
               await Task.Delay(Timeout.Infinite);
            }
            catch (TaskCanceledException)
            {
               Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Application shutown requested");
            }

            await _client.DisconnectAsync();
         }
      }
      catch (Exception ex)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Application startup failure {ex.Message}", ex);
      }

      Console.WriteLine("Press Enter to exit");
      Console.ReadLine();
   }

   private static void OnMessageReceived(object? sender, HiveMQtt.Client.Events.OnMessageReceivedEventArgs e)
   {
      HiveMQClient client = (HiveMQClient)sender!;

      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.receive start");
      Console.WriteLine($" Topic:{e.PublishMessage.Topic} QoS:{e.PublishMessage.QoS} Payload:{e.PublishMessage.PayloadAsString}");

      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.Publish start");
      foreach (string topic in _applicationSettings.PublishTopics.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
      {
         e.PublishMessage.Topic = string.Format(topic, _applicationSettings.ClientId);

         var transformer = _scriptEngine.GetTransformer();

         if (transformer is null)
         {
            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Transformer is null");
            return;
         }

         var transformedMessages = transformer.Transform(e.PublishMessage);
         if (transformedMessages is null)
         {
            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Transformer returned null");
            return;
         }

         if (transformedMessages.Length == 0)
         {
            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Transformer returned no messages");
            return;
         }

         foreach (MQTT5PublishMessage message in transformer.Transform(e.PublishMessage))
         {
            if (message is null)
            {
               Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Transformer message is null");

               continue;
            }

            try
            {
               Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Topic:{e.PublishMessage.Topic} HiveMQ Publish start ");

               var resultPublish = client.PublishAsync(message).GetAwaiter().GetResult();

               Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Published:{resultPublish.QoS1ReasonCode} {resultPublish.QoS2ReasonCode}");
            }
            catch (Exception ex)
            {
               Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ Publish exception {ex.Message}");
            }
         }
      }
      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.Receive finish");
   }
}