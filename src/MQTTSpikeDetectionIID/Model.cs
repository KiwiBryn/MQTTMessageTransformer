using devMobile.IoT.MqttTransformers;
using HiveMQtt.MQTT5.Types;
using Microsoft.ML.Data;
using System.Collections.Concurrent;

namespace devMobile.IoT.MqttTransformer.Detection.Model
{
   public enum DetectionMode
   {
      Undefined = 0,
      SpikeID = 1,
      spikeSSA = 2,
   }

   public sealed class TimeSeriesData
   {
      public float Value { get; set; }
   }

   public sealed class SpikePrediction
   {
      // IID spike: [isSpike, rawScore, pValue]
      [VectorType(3)]
      public double[] Prediction { get; set; } = default!;
   }

   internal class ApplicationSettings
   {
      public TimeSpan PublicationTimerDue { get; set; } = TimeSpan.FromSeconds(10);
      public TimeSpan PublicationTimerPeriod { get; set; } = TimeSpan.FromSeconds(30);

      public required string ClientId { get; set; } = string.Empty;
      public required string Host { get; set; } = string.Empty;
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
      public QualityOfService InputQualityOfService { get; set; } = QualityOfService.AtLeastOnceDelivery;

      public string OutputTopic { get; set; } = string.Empty;

      public QualityOfService OutputQualityOfService { get; set; } = QualityOfService.AtLeastOnceDelivery;

      public string ContentType { get; set; } = string.Empty;

      public string InputMessageTransformFile { get; set; } = string.Empty;
      public IInputMessageTransformer? InputMessageTransformer { get; set; } = null;

      public string OutputMessageTransformFile { get; set; } = string.Empty;
      public ISpikeOutputMessageTransformer? OutputMessageTransformer { get; set; } = null;
   
      // ---- IID spike tuning knobs (per topic) ----
      public int PValueHistoryLength { get; set; } = 32;  // >= 2; typical 16–64
      public double Confidence { get; set; } = 95.0; // 0–100; higher => fewer spikes
   }
}
