//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
using devMobile.IoT.MqttTransformers;

namespace devMobile.IoT.MqttTransformer.Detection.Model
{
   public enum DetectionMode
   {
      Undefined = 0,
      IID = 1,
      SSA = 2,
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
      public required string ClientId { get; set; } = string.Empty;
      public required string Host { get; set; } = string.Empty;
      public int Port { get; set; } = 8883;
      public bool CleanStart { get; set; } = true;
      public bool UseTls { get; set; } = true;
      public bool AutomaticReconnect { get; set; } = true;

#if HIVEMQ_USERNAME_AND_PASSWORD_SUPPORT      
      public required string UserName { get; set; } = string.Empty;
      public required string Password { get; set; } = string.Empty;
#endif

#if HIVEMQ_CERTIFICATE_SUPPORT
      public required string ClientCertificateFileName { get; set; } = string.Empty;
      public required string ClientCertificatePassword { get; set; } = string.Empty;
#endif

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

      public DetectionMode DetectionMode { get; set; } = DetectionMode.Undefined;

      public required IIDSettings IIDSettings { get; set; } = new IIDSettings();

      public required SSASettings SSASettings { get; set; } = new SSASettings();
   }

   public class IIDSettings
   {
      public double Confidence { get; set; } = 95.0; // 0–100; higher => fewer spikes
      public int PValueHistoryLength { get; set; } = 32;  // >= 2; typical 16–64

      // (Positive) Only positive anomalies are detected.(Negative) Only negative anomalies are detected , (TwoSided) Both positive and negative anomalies are detected.
      public AnomalySide AnomalySide { get; set; } = AnomalySide.TwoSided;
   }

   // SSA-specific tuning knobs (per topic)
   public class SSASettings
   {
      public double Confidence { get; set; } = 95.0; // 0–100; higher => fewer spikes
      public int PValueHistoryLength { get; set; } = 32;  // >= 2; typical 16–64
      public int SeasonalityWindowSize { get; set; } = 32;  // >= 2; typical 16–64
      public int TrainingWindowSize { get; set; } = 32;  // >= 2; typical 16–64

      // (Positive) Only positive anomalies are detected (Negative) Only negative anomalies are detected (TwoSided) Both positive and negative anomalies are detected.
      public AnomalySide AnomalySide { get; set; } = AnomalySide.TwoSided;
   }
}
