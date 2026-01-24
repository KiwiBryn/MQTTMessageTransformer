//---------------------------------------------------------------------------------
// Copyright (c) January 2025, devMobile Software
//
using HiveMQtt.MQTT5.Types;

namespace devMobile.IoT.MqttTransformer.CSScriptLoopback.Model;


internal class ApplicationSettings
{
   public TimeSpan PublicationTimerDue { get; set; } = TimeSpan.FromSeconds(10);
   public TimeSpan PublicationTimerPeriod { get; set; } = TimeSpan.FromSeconds(30);

   public required string ClientId { get; set; } = string.Empty;
   public required string Host { get; set; } = "";
   public int Port { get; set; } = 8883;
   public bool CleanStart { get; set; } = true;
   public bool UseTls { get; set; } = true;

   public required string PublishTopics { get; set; } = string.Empty;
   public QualityOfService PublishQualityOfService { get; set; } = QualityOfService.AtLeastOnceDelivery;
   public required string PublishContentType { get; set; } = "";

   public required string SubscribeTopics { get; set; } = string.Empty;
   public QualityOfService SubscribeQualityOfService { get; set; } = QualityOfService.AtLeastOnceDelivery;

   public required string UserName { get; set; } = string.Empty;

   public required string Password { get; set; } = string.Empty;

   public required string ClientCertificateFileName { get; set; } = string.Empty;

   public required string ClientCertificatePassword { get; set; } = string.Empty;
}

