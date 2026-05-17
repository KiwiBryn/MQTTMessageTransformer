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

   public sealed class ChangePointPrediction
   {
      // Change point: [alert, rawScore, pValue, martingaleValue]
      [VectorType(4)]
      public double[] Prediction { get; set; } = default!;
   }

   internal class ApplicationSettings
   {
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
      public IChangePointOutputMessageTransformer? OutputMessageTransformer { get; set; } = null;

      public int changeHistoryLength { get; set; }  
      public double Confidence { get; set; }

      public DetectionMode DetectionMode { get; set; } = DetectionMode.Undefined;

      // SSA-specific tuning knobs (per topic)
      public int TrainingWindowSize { get; set; } = 32; 
      public int SeasonalityWindowSize { get; set; } = 32; 
   }
}
