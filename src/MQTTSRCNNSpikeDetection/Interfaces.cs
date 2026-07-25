//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
namespace devMobile.IoT.MqttTransformers
{
   public interface IInputMessageTransformer
   {
      public float Transform(string topic, byte[] payload);
   }

   public interface ISpikeOutputMessageTransformer
   {
      public byte[] Transform(string sourceTopic, double value, double rawScore, double magnitude);
   }
}
