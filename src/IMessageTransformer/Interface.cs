using HiveMQtt.MQTT5.Types;


public interface IMessageTransformer
{
   public MQTT5PublishMessage[] Transform(MQTT5PublishMessage mqttPublishMessage);
}
