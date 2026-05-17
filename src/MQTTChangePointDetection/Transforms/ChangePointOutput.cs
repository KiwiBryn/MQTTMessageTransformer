using devMobile.IoT.MqttTransformers;
using System.Text.Json;


public class ChangePointOutputTransformer : IChangePointOutputMessageTransformer
{
   public byte[] Transform(string sourceTopic, double value, double rawScore, double PValue, double martingale)
   {
      var obj = new
      {
         ChangePoint = true,
         value,
         sourceTopic,
         rawScore,
         PValue,
         martingale,
      };

      string payload = JsonSerializer.Serialize(obj, new JsonSerializerOptions
      {
         DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
      });

      return System.Text.Encoding.UTF8.GetBytes(payload);
   }
}