using devMobile.IoT.MqttTransformers;
using System.Text.Json;
using System.Text.Json.Serialization;
using System;


public class SpikeOutputTransformer : ISpikeOutputMessageTransformer
{
   public byte[] Transform(string topic, double rawScore, double pValue)
   {
      var obj = new
      {
         topic,
         rawScore,
         pValue
      };

      string payload =  JsonSerializer.Serialize(obj, new JsonSerializerOptions
      {
         DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
      });

      return System.Text.Encoding.UTF8.GetBytes(payload);
   }
}