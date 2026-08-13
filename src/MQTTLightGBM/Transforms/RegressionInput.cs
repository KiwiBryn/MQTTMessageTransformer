//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
/*
{
"temp":10.395078,
"humidity":84.45233,
"par":0,
"hourSin":0.21643962,
"hourCos":0.976296,
"soilMoisture":39.60148
*/
using System; // DO NOT remove this as required for InvalidOperationException
using devMobile.IoT.MqttTransFormer.LightGBM;

internal class SKU12345
{
   public string ClientID { get; set; } = string.Empty;
   public float temp { get; set; }
   public float humidity { get; set; }
   public float par { get; set; }
   public float hourSin { get; set; }
   public float hourCos { get; set; }
   public float soilMoisture { get; set; }
}

public class InputTransformerRegression : IInputMessageTransformer
{
   public float[] Transform(string topic, byte[] payload)
   {
      var json = System.Text.Encoding.UTF8.GetString(payload);

      var obj = System.Text.Json.JsonSerializer.Deserialize<SKU12345>(json) ?? throw new InvalidOperationException("Failed to deserialize payload");

      return new float[] { obj.temp, obj.humidity, obj.par, obj.hourSin, obj.hourCos, obj.soilMoisture };
   }
}
