//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
/*
 {
   "ClientID:"Device12345",
   "Mm":269,
   "Cm":26.8999996,
   "Temperature":24.2999992
   }
*/
using devMobile.IoT.MqttTransformers;


internal class SKU101991042
{    
   public string ClientID { get; set; } = string.Empty;
   public int Mm { get; set; }
   public float Cm { get; set; }
   public float Temperature { get; set; }
}

public class InputSKU101991042 : IInputMessageTransformer
{
   public float Transform(string topic, byte[] payload)
   {
      var json = System.Text.Encoding.UTF8.GetString(payload);

      var obj = System.Text.Json.JsonSerializer.Deserialize<SKU101991042>(json);

      return obj.Cm; 
   }
}