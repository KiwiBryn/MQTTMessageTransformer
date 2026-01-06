//---------------------------------------------------------------------------------
// Copyright (c) January 2025, devMobile Software
//
using HiveMQtt.MQTT5.Types;


namespace devMobile.IoT.NanoMqtt.Model
{
	internal class ApplicationSettings
	{
		public TimeSpan PublicationTimerDue { get; set; }
		public TimeSpan PublicationTimerPeriod { get; set; }

      public string ClientId { get; set; }
      public string Host{ get; set; }
      public int Port { get; set; }
		public bool CleanStart { get; set; }

      public string PublishTopic { get; set; }
      public QualityOfService PublishQualityOfService { get; set; }
      public string PublishContentType { get; set; }

      public string SubscribeTopics { get; set; }
      public QualityOfService SubscribeQualityOfService { get; set; }

      public string UserName { get; set; }
      
      public string Password { get; set; }

      public string ClientCertificateFileName { get; set; }

      public string ClientCertificatePassword { get; set; }
   }
}
