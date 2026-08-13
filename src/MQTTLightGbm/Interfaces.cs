//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
using devMobile.IoT.MqttTransFormer.LightGBM.Model;


namespace devMobile.IoT.MqttTransFormer.LightGBM
{
   public interface IInputMessageTransformer
   {
      public float[] Transform(string topic, byte[] payload);
   }

   public interface IOutputMessageBinaryTransformer
   {
      public byte[] Transform(string sourceTopic, PredictionBinary predictionBinary);
   }

   public interface IOutputMessageClassificationTransformer
   {
      public byte[] Transform(string sourceTopic, PredictionMultiClass predictionMultiClass);
   }

   public interface IOutputMessageRegressionTransformer
   {
      public byte[] Transform(string sourceTopic, PredictionRegression predictionRegression);
   }
}
