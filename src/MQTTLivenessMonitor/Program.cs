//---------------------------------------------------------------------------------
// Copyright (c) February 2025, devMobile Software
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// https://github.com/hivemq/hivemq-mqtt-client-dotnet 
//
using CSScriptLib;


namespace devMobile.IoT.MqttTransformer.MQTTLivenessMonitor;


class Program
{
   private static Model.ApplicationSettings _applicationSettings;
   private static HiveMQClient? _client;

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

         // Compile the message transformer scripts and assign to the topic configuration
         foreach (var subscribedTopic in _applicationSettings.SubscribedTopics.Values)
         {
            subscribedTopic.StoppedMessageTransformer = CSScript.Evaluator.LoadFile<IStoppedMessageTransformer>(subscribedTopic.StoppedMessageTransformFile);
            subscribedTopic.ResumedMessageTransformer = CSScript.Evaluator.LoadFile<IResumedMessageTransformer>(subscribedTopic.ResumedMessageTransformFile);
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
            foreach (var subscribedTopic in _applicationSettings.SubscribedTopics)
            {
               var subscribeResult = await _client.SubscribeAsync(subscribedTopic.Key, subscribedTopic.Value.InputQualityOfService);

               Console.WriteLine($" Topic:{subscribedTopic.Key} Result:{subscribeResult.Subscriptions[0].SubscribeReasonCode}");
            }

            Console.WriteLine($"Timer Due:{_applicationSettings.PublicationTimerDue} Period:{_applicationSettings.PublicationTimerPeriod}");

            Timer imageUpdatetimer = new(PublisherTimerCallback, _client, _applicationSettings.PublicationTimerDue, _applicationSettings.PublicationTimerPeriod);

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


   static bool TimerRunning = false;


   private static async void PublisherTimerCallback(object? state)
   {
      if (TimerRunning || state is null)
      {
         return;
      }

      HiveMQClient client = (HiveMQClient)state;
      DateTime now = DateTime.UtcNow;

      try
      {
         foreach (var subscribedTopic in _applicationSettings.SubscribedTopics)
         {
            var subscribedTopicValue = subscribedTopic.Value;

            if (subscribedTopicValue.IsStopped)
            {
               continue;
            }

            if ((subscribedTopicValue.LastMessageReceived + subscribedTopicValue.MaximumDelay) > now)
            {
               Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Message received for topic {subscribedTopic.Key} in the last {subscribedTopicValue.MaximumDelay}");

               continue;
            }

            subscribedTopicValue.IsStopped = true;

            MQTT5PublishMessage message = new MQTT5PublishMessage(subscribedTopicValue.OutputTopic, subscribedTopicValue.OutputQualityOfService)
            {
               ContentType = subscribedTopicValue.ContentType,
               PayloadAsString = subscribedTopicValue.StoppedMessageTransformer.Transform(subscribedTopic.Key, subscribedTopicValue.LastMessageReceived, subscribedTopicValue.MaximumDelay)
            };
            var resultPublish = client.PublishAsync(message).GetAwaiter().GetResult();

            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Published:{resultPublish.QoS1ReasonCode} {resultPublish.QoS2ReasonCode}");

            Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Message not received for topic {subscribedTopic.Key} in the last {subscribedTopicValue.MaximumDelay}");
         }
      }
      catch (Exception ex)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} PublisherTimerCallback failure {ex.Message}", ex);
      }
      finally
      {
         TimerRunning = false;
      }
   }

   private static void OnMessageReceived(object? sender, HiveMQtt.Client.Events.OnMessageReceivedEventArgs e)
   {
      DateTime now = DateTime.UtcNow;
      HiveMQClient client = (HiveMQClient)sender!;

      if (e.PublishMessage.Topic is null)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.receive received message with null topic");
         return;
      }

      //Console.WriteLine($"{now:yy-MM-dd HH:mm:ss:fff} HiveMQ.receive start Topic:{e.PublishMessage.Topic} QoS:{e.PublishMessage.QoS}");

      if (!_applicationSettings.SubscribedTopics.TryGetValue(e.PublishMessage.Topic, out Model.TopicConfiguration? subscribedTopic))
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Received message for unsubscribed topic {e.PublishMessage.Topic}");
         return;
      }

      if (subscribedTopic.IsStopped)
      {
         MQTT5PublishMessage message = new MQTT5PublishMessage(subscribedTopic.OutputTopic, subscribedTopic.OutputQualityOfService)
         {
            ContentType = subscribedTopic.ContentType,
            PayloadAsString = subscribedTopic.ResumedMessageTransformer.Transform(e.PublishMessage.Topic, subscribedTopic.MaximumDelay)
         };
         var resultPublish = client.PublishAsync(message).GetAwaiter().GetResult();

         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} Published:{resultPublish.QoS1ReasonCode} {resultPublish.QoS2ReasonCode}");
      }

      subscribedTopic.IsStopped = false;
      subscribedTopic.LastMessageReceived = now;

      //Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss:fff} HiveMQ.receive finish");
   }
}
