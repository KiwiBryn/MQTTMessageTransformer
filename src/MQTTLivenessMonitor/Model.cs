//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
using System.Collections.Concurrent;

namespace devMobile.IoT.MqttTransformer.MQTTLivenessMonitor.Model;


internal class ApplicationSettings
{
   public TimeSpan PublicationTimerDue { get; set; } = TimeSpan.FromSeconds(10);
   public TimeSpan PublicationTimerPeriod { get; set; } = TimeSpan.FromSeconds(30);

   public required string ClientId { get; set; } = string.Empty;
   public required string Host { get; set; } = "";
   public int Port { get; set; } = 8883;
   public bool CleanStart { get; set; } = true;
   public bool UseTls { get; set; } = true;

   public required string UserName { get; set; } = string.Empty;
   public required string Password { get; set; } = string.Empty;
   public required string ClientCertificateFileName { get; set; } = string.Empty;
   public required string ClientCertificatePassword { get; set; } = string.Empty;

   public required ConcurrentDictionary<string, TopicConfiguration> SubscribedTopics { get; set; }
}

internal class TopicConfiguration
{
   public TimeSpan MaximumDelay { get; set; } = new TimeSpan(0, 0, 0);

   public string StoppedMessageTransformFile { get; set; } = string.Empty;

   public IStoppedMessageTransformer? StoppedMessageTransformer { get; set; } = null;

   public string ResumedMessageTransformFile { get; set; } = string.Empty;

   public IResumedMessageTransformer? ResumedMessageTransformer { get; set; } = null;

   public QualityOfService InputQualityOfService { get; set; } = QualityOfService.AtLeastOnceDelivery;

   public string OutputTopic { get; set; } = string.Empty;

   public QualityOfService OutputQualityOfService { get; set; } = QualityOfService.AtLeastOnceDelivery;

   public DateTime LastMessageReceived { get; set; } = DateTime.UtcNow;

   public string ContentType { get; set; } = string.Empty;

   public bool IsStopped { get; set; } = false;
}


