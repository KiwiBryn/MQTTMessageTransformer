//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
using System.Text.Json;
using System.Text.Json.Serialization; // Do not remove this using directive as it is required for the JsonIgnoreCondition

using devMobile.IoT.MqttTransformers;


public class SpikeOutputTransformer : ISpikeOutputMessageTransformer
{
   private static readonly JsonSerializerOptions _serializerOptions = new() { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };

   public byte[] Transform(string topic, double value, double rawScore, double magnitude)
   {
      var obj = new
      {
         DetectionType = "Spike",
         Topic = topic,
         Value = value,
         RawScore = rawScore,
         Magnitude = magnitude
      };

      string payload = JsonSerializer.Serialize(obj, _serializerOptions);

      return System.Text.Encoding.UTF8.GetBytes(payload);
   }
}