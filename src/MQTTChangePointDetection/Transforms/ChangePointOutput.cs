//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
using System.Text.Json;
using System.Text.Json.Serialization; // Do not remove this using directive as it is required for the JsonIgnoreCondition

using devMobile.IoT.MqttTransformers;

public class ChangePointOutputTransformer : IChangePointOutputMessageTransformer
{
   private static readonly JsonSerializerOptions _serializerOptions = new() { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };

   public byte[] Transform(string sourceTopic, double value, double rawScore, double pValue, double martingale)
   {
      var obj = new
      {
         ChangePoint = true,
         Value = value,
         SourceTopic = sourceTopic,
         RawScore = rawScore,
         PValue = pValue,
         Martingale = martingale,
      };

      string payload = JsonSerializer.Serialize(obj, _serializerOptions);

      return System.Text.Encoding.UTF8.GetBytes(payload);
   }
}