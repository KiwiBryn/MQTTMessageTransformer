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

      public required string ClientId { get; set; }
      public required string Host{ get; set; }
      public int Port { get; set; }
		public bool CleanStart { get; set; }
      public bool UseTls { get; set; }

      public required string PublishTopics { get; set; }
      public QualityOfService PublishQualityOfService { get; set; }
      public required string PublishContentType { get; set; }

      public required string SubscribeTopics { get; set; }
      public QualityOfService SubscribeQualityOfService { get; set; }

      public required string UserName { get; set; }
      
      public required string Password { get; set; }

      public required string ClientCertificateFileName { get; set; }

      public required string ClientCertificatePassword { get; set; }
   }
}
