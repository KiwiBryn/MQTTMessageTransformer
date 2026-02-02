using HiveMQtt.MQTT5.Types;

public class returnsNullmessageTransformer : IMessageTransformer
{
   public MQTT5PublishMessage[] Transform(MQTT5PublishMessage message)
   {
      if (message.Payload is null)
      {
         return [];
      }

      return [null];
   }
}
