//---------------------------------------------------------------------------------
// Copyright (c) January 2025, devMobile Software
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// https://github.com/hivemq/hivemq-mqtt-client-dotnet 
//
namespace devMobile.IoT.MqttTransformer.CodeLoopback;


class Program
{
   private static Model.ApplicationSettings _applicationSettings;

   static async Task Main()
   {
      HiveMQClient _client;

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

         using (_client = new HiveMQClient(optionsBuilder.Build()))
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

         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Topic:{e.PublishMessage.Topic} HiveMQ Publish start ");
         var resultPublish = client.PublishAsync(e.PublishMessage).GetAwaiter().GetResult();
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Published:{resultPublish.QoS1ReasonCode} {resultPublish.QoS2ReasonCode}");
      }
      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.Receive finish");
   }
}
