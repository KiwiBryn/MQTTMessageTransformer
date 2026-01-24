//---------------------------------------------------------------------------------
// Copyright (c) January 2025, devMobile Software
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// https://github.com/hivemq/hivemq-mqtt-client-dotnet 
//
using CSScriptLib;

using HiveMQtt.Client;
using HiveMQtt.MQTT5.ReasonCodes;
using HiveMQtt.MQTT5.Types;


namespace devMobile.IoT.MqttTransformer.CSScriptLoopback;

public interface IMessageTransformer
{
   public MQTT5PublishMessage[] Transform(MQTT5PublishMessage mqttPublishMessage);
}


class Program
{
   private static Model.ApplicationSettings _applicationSettings;
   private static IMessageTransformer _evaluator;

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

         _evaluator = CSScript.Evaluator.LoadCode<IMessageTransformer>(sampleTransformCode);

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

         foreach (MQTT5PublishMessage message in _evaluator.Transform(e.PublishMessage))
         {
            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Topic:{e.PublishMessage.Topic} HiveMQ Publish start ");
            var resultPublish = client.PublishAsync(message).GetAwaiter().GetResult();
            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Published:{resultPublish.QoS1ReasonCode} {resultPublish.QoS2ReasonCode}");
         }
      }
      Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.Receive finish");
   }

   // This code is compiled as the application starts up, it implements the IMessageTransformer interface
   const string sampleTransformCode = @"
      using System.Text;
      using HiveMQtt.MQTT5.Types;

      public class messageTransformer : devMobile.IoT.MqttTransformer.CSScriptLoopback.IMessageTransformer
      {
         public MQTT5PublishMessage[] Transform(MQTT5PublishMessage message)
         {
            // Example: echo the payload to a new topic
            var payload = Encoding.UTF8.GetString(message.Payload);


            // Simple transformations: convert to uppercase or lowercase
            var toLower = new MQTT5PublishMessage
            {
               Topic = message.Topic,
               Payload = Encoding.UTF8.GetBytes(payload.ToLower()),
               QoS = QualityOfService.AtLeastOnceDelivery
            };

            var toUpper = new MQTT5PublishMessage
            {
               Topic = message.Topic,
               Payload = Encoding.UTF8.GetBytes(payload.ToUpper()),
               QoS = QualityOfService.AtLeastOnceDelivery
            };

            return new[] { toLower, toUpper };
         }
      }";
}