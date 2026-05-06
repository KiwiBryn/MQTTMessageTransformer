using System.Text.Json;

using devMobile.IoT.MqttTransformers;

public class SKU101991029 : IInputMessageTransformer
{
   public float Transform(string topic, byte[] payload)
   {
      return 1.23f; // Placeholder: replace with actual parsing logic
   }
}


   /*
   public InputExtractResult Transform(string topic, byte[] payload)
   {
      var json = System.Text.Encoding.UTF8.GetString(payload);

      string deviceId = "unknown";
      float value = 0f;

      try
      {
         using var doc = JsonDocument.Parse(json);
         var root = doc.RootElement;

         if (root.ValueKind == JsonValueKind.Object)
         {
            if (root.TryGetProperty("deviceId", out var d))
               deviceId = d.GetString() ?? "unknown";

            // CO2 examples: "co2", "ppm", or generic "value"
            if (root.TryGetProperty("co2", out var co2) && co2.ValueKind == JsonValueKind.Number)
               value = co2.TryGetSingle(out var s) ? s : (float)co2.GetDouble();
            else if (root.TryGetProperty("ppm", out var ppm) && ppm.ValueKind == JsonValueKind.Number)
               value = ppm.TryGetSingle(out var s2) ? s2 : (float)ppm.GetDouble();
            else if (root.TryGetProperty("value", out var generic) && generic.ValueKind == JsonValueKind.Number)
               value = generic.TryGetSingle(out var s3) ? s3 : (float)generic.GetDouble();
         }
         else if (root.ValueKind == JsonValueKind.Number)
         {
            value = root.TryGetSingle(out var s4) ? s4 : (float)root.GetDouble();
         }
      }
      catch
      {
         // Fallback: payload as plain number string
         if (float.TryParse(json, out var parsed))
            value = parsed;
      }

      return new InputExtractResult(deviceId, value);
   }
   */
