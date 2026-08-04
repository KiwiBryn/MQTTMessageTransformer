//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
/*
{
  "ClientID:"Device123",
  "Co2PPM":456,
  "Temperature":24.2999992,
  "RelativeHumidity":54.0,
  "WarmUpTime":120
}
*/
using System.Text.Json;

using devMobile.IoT.MqttTransformers;

public class SKU101991029 : IInputMessageTransformer
{
   public float[] Transform(string topic, byte[] payload)
   {
      return new float[] { 123, 1.23f, 32.1f };
   }
}
