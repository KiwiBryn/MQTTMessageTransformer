//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
using System.Text.Json;

using devMobile.IoT.MqttTransformers;

public class SKU101991029 : IInputMessageTransformer
{
   public float Transform(string topic, byte[] payload)
   {
      return 1.23f; // Placeholder: replace with actual parsing logic
   }
}


