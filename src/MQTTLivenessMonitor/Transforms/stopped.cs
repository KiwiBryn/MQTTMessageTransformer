//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
using devMobile.IoT.MqttTransformer.MQTTLivenessMonitor;

public class Stopped : IStoppedMessageTransformer
{
   public string Transform(string topic, DateTime lastMessageReceivedUtc, TimeSpan maximumDelay)
   {   
      return $"No message received since {lastMessageReceivedUtc} for topic {topic} in the last {maximumDelay:hh\\:mm\\:ss}";
   }
}
