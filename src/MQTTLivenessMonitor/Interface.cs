//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
namespace devMobile.IoT.MqttTransformer.MQTTLivenessMonitor;

public interface IStoppedMessageTransformer
{
   public string Transform(string topic, DateTime lastMessageReceivedUtc, TimeSpan maximumDelay);
}

public interface IResumedMessageTransformer
{
   public string Transform(string topic, TimeSpan maximumDelay);
}
