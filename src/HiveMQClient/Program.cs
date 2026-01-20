//---------------------------------------------------------------------------------
// Copyright (c) January 2025, devMobile Software
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// https://github.com/hivemq/hivemq-mqtt-client-dotnet 
//
using HiveMQtt.Client;
using HiveMQtt.MQTT5.ReasonCodes;
using HiveMQtt.MQTT5.Types;

namespace devMobile.IoT.MqttTransformer.Client;


class Program
{
   private static Model.ApplicationSettings _applicationSettings;
   private static HiveMQClient _client;
   private static bool _publisherBusy = false;

   static async Task Main()
   {
      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Hive MQ client starting");

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

               var subscribeResult = await _client.SubscribeAsync(topicFormatted, _applicationSettings.SubscribeQualityOfService);

               Console.WriteLine($" Topic:{topicFormatted} Result:{subscribeResult.Subscriptions[0].SubscribeReasonCode}");
            }

            Console.WriteLine($"Timer Due:{_applicationSettings.PublicationTimerDue} Period:{_applicationSettings.PublicationTimerPeriod}");

            Timer imageUpdatetimer = new(PublisherTimerCallback, null, _applicationSettings.PublicationTimerDue, _applicationSettings.PublicationTimerPeriod);

            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} press <ctrl^c> to exit");

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


   private static async void PublisherTimerCallback(object? state)
   {
      // Just incase - stop code being called while publish already in progress
      if (_publisherBusy)
      {
         return;
      }
      _publisherBusy = true;

      var payload = JsonSerializer.Serialize(new
      {
         Content = $"{DateTime.UtcNow:yy-MM-dd HH:mm:ss}",
      });

      try
      {
         Console.WriteLine($"Publishing to Topic(s)");
         foreach (string topic in _applicationSettings.PublishTopics.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
         {
            string topicFormatted = string.Format(topic, _applicationSettings.ClientId);

            var message = new MQTT5PublishMessage
            {
               Topic = topicFormatted,
               Payload = Encoding.ASCII.GetBytes(payload),
               ContentType = _applicationSettings.PublishContentType,
               QoS = _applicationSettings.PublishQualityOfService,
            };

            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.Publish start");

            var resultPublish = await _client.PublishAsync(message);

            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.Publish finish");

            Console.WriteLine($" Topic:{message.Topic} Reason:{resultPublish.QoS1ReasonCode}{resultPublish.QoS2ReasonCode}");
         }
      }
      catch (Exception ex)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} HiveMQ.Publish failed {ex.Message}");
      }
      finally
      {
         _publisherBusy = false;
      }
   }

   private static void OnMessageReceived(object? sender, HiveMQtt.Client.Events.OnMessageReceivedEventArgs e)
   {
      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.receive start");
      Console.WriteLine($" Topic:{e.PublishMessage.Topic} QoS:{e.PublishMessage.QoS} Payload:{e.PublishMessage.PayloadAsString}");
      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.receive finish");
   }
}

/*
    optionsBuilder.WithClientCertificate(_applicationSettings.ClientCertificateFileName, _applicationSettings.ClientCertificatePassword);
   /*
   SecureString password = new SecureString();
   foreach (char c in _applicationSettings.ClientCertificatePassword)
   {
      password.AppendChar(c);
   }
   password.MakeReadOnly();

   optionsBuilder.WithClientCertificate(_applicationSettings.ClientCertificateFileName, password);

}

//if (!string.IsNullOrWhiteSpace(_applicationSettings.Password))
if (_applicationSettings.Password.Length > 0)
{
   optionsBuilder = optionsBuilder.WithPassword(_applicationSettings.Password);
   /*
   SecureString password = new SecureString();
   foreach (char c in _applicationSettings.Password)
   {
      password.AppendChar(c);
   }
   password.MakeReadOnly();

   optionsBuilder = optionsBuilder.WithPassword(password);
   */