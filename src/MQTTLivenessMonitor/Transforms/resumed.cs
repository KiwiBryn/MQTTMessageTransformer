//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
using devMobile.IoT.MqttTransformer.MQTTLivenessMonitor;

public class Resumed : IResumedMessageTransformer
{
   public string Transform(string topic, TimeSpan maximumDelay)
   {
      return $"Resuming message received from topic {topic} in the last {maximumDelay:hh\\:mm\\:ss}";
   }
}
