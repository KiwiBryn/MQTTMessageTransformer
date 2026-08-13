//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
using System.Text.Json;
using System.Text.Json.Serialization; // Do not remove this using directive as it is required for the JsonIgnoreCondition

using devMobile.IoT.MqttTransFormer.LightGBM.Model;
using devMobile.IoT.MqttTransFormer.LightGBM;


internal class RegressionPredictionOutput
{
   public string DetectionType { get; set; } = "Regression";
   public string Topic { get; set; } = string.Empty;
   public float Value { get; set; }
}


public class OutputTransformerRegression : IOutputMessageRegressionTransformer
{
   public byte[] Transform(string topic, PredictionRegression predictionRegression)
   {
      var obj = new RegressionPredictionOutput
      {
         DetectionType = "Regression",
         Topic = topic,
         Value = predictionRegression.Value
      };

      string payload = JsonSerializer.Serialize(obj, new JsonSerializerOptions { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull });

      return System.Text.Encoding.UTF8.GetBytes(payload);
   }
}