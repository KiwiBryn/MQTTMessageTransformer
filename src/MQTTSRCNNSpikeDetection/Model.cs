//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
using devMobile.IoT.MqttTransformers;
using HiveMQtt.MQTT5.Types;
using Microsoft.ML.Data;
using System.Collections.Concurrent;

namespace devMobile.IoT.MqttTransformer.Detection.Model
{
   public sealed class TimeSeriesData
   {
      public float Value { get; set; }
   }

   public sealed class SpikePrediction
   {
      // SRCNN spike: [isSpike, rawScore, magnitude] Could be 4 or 7 if AnomalyAndExpectedValue or AnomalyAndMargin used
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

      public required SrCnnSettings SrCnnSettings { get; set; } = new SrCnnSettings();
   }

   /// <summary>
   /// Configuration for ML.NET SR-CNN (Spectral Residual CNN) spike/anomaly detection.
   /// Defaults follow "Scenario A — General use" and are safe for most IoT telemetry.
   /// </summary>
   public class SrCnnSettings
   {
      /// <summary>
      /// Sliding window used by the spectral residual step. Must be >= 12.
      /// Larger = smoother / fewer false positives, slower to react.
      /// </summary>
      public int WindowSize { get; set; } = 64;

      /// <summary>
      /// Points appended before FFT to reduce edge effects.
      /// Must be >= 2 and &lt; <see cref="WindowSize"/>.
      /// </summary>
      public int BackAddWindowSize { get; set; } = 5;

      /// <summary>
      /// Points used to estimate the trailing average.
      /// </summary>
      public int LookaheadWindowSize { get; set; } = 5;

      /// <summary>
      /// Window for the moving average in the SR step.
      /// </summary>
      public int AveragingWindowSize { get; set; } = 3;

      /// <summary>
      /// Window used to compute the anomaly score / p-value.
      /// Must be >= 1 and &lt;= <see cref="WindowSize"/>.
      /// </summary>
      public int JudgementWindowSize { get; set; } = 21;

      /// <summary>
      /// Sensitivity threshold. Range 0.0 - 1.0.
      /// Lower = more sensitive (more spikes), higher = stricter.
      /// </summary>
      public double Threshold { get; set; } = 0.3;

      /// <summary>
      /// Only used when <see cref="DetectMode"/> = AnomalyAndMargin. Range 0 - 100.
      /// Higher = tighter margin band.
      /// </summary>
      public double Sensitivity { get; set; } = 99.0;
      
      /// <summary>
      /// Minimum buffered points required before scoring begins.
      /// Defaults to <see cref="WindowSize"/>.
      /// </summary>
      public int? WarmupPoints { get; set; }

      /// <summary>
      /// Maximum points to retain in the per-topic rolling buffer.
      /// Defaults to 2 * <see cref="WindowSize"/>.
      /// </summary>
      public int? MaxBufferSize { get; set; }
   }
}
