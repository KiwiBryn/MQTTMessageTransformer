//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
using System.Text.Json;
using System.Text.Json.Serialization; // Do not remove this using directive as it is required for the JsonIgnoreCondition
using devMobile.IoT.MqttTransformer.Detection.Model;
using devMobile.IoT.MqttTransformers;


public class OutputTransformerBinary : IOutputMessageBinaryTransformer
{
   private static readonly JsonSerializerOptions _serializerOptions = new() { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };

   public byte[] Transform(string topic, PredictionBinary predictionBinary)
   {
      var obj = new
      {
         topic,
         DetectionType = "Binary",
         predictionBinary.Score,
         predictionBinary.PredictedLabel,
         predictionBinary.Probability
      };

      string payload = JsonSerializer.Serialize(obj, _serializerOptions);

      return System.Text.Encoding.UTF8.GetBytes(payload);
   }
}