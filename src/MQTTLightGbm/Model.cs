//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
using HiveMQtt.MQTT5.Types;

namespace devMobile.IoT.MqttTransFormer.LightGBM.Model
{
   // These classes are used to define the input and output schema LightGBM model. The input schema is defined by the ModelInput class,
   // which contains a single property Features of type float array. The output schema is defined by the PredictionRegression, PredictionMultiClass,
   // and PredictionBinary classes, which contain properties for the predicted score and label.
   public class ModelInput
   {
      [VectorType(6)]
      public float[] Features{ get; set; } = new float[6];
   }

   public class PredictionRegression
   {
      [ColumnName("Score")]
      public float Value { get; set; }
   }

   public class PredictionMultiClass
   {
      public string PredictedLabel { get; set; } = string.Empty;

      public float[] Score { get; set; } = [];
   }

   public class PredictionBinary
   {
      public bool PredictedLabel { get; set; }
      public float Probability { get; set; }
      public float Score { get; set; }
   }

   public enum ModelType
   {
      Undefined = 0,
      Regression = 1,
      BinaryClassification = 2,
      MultiClassClassification = 3
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

      public string ModelFileName { get; set; } = string.Empty;

      public string OutputTopic { get; set; } = string.Empty;

      public QualityOfService OutputQualityOfService { get; set; } = QualityOfService.AtLeastOnceDelivery;

      public string ContentType { get; set; } = string.Empty;

      public string InputMessageTransformFile { get; set; } = string.Empty;
      public IInputMessageTransformer? InputMessageTransformer { get; set; } = null;

      public string OutputMessageTransformFile { get; set; } = string.Empty;
      public IOutputMessageBinaryTransformer? OutputMessageBinaryTransformer { get; set; } = null;
      public IOutputMessageRegressionTransformer? OutputMessageRegressionTransformer { get; set; } = null;
      public IOutputMessageClassificationTransformer? OutputMessageClassificationTransformer { get; set; } = null;

      public ModelType ModelType { get; set; } = ModelType.Undefined;
   }
}
