using HiveMQtt.MQTT5.Types;
using System.Text;

public class returnsNullTransformer : IMessageTransformer
{
   public MQTT5PublishMessage[] Transform(MQTT5PublishMessage message)
   {
      if (message.Payload is null)
      {
         return [];
      }

      return null;
   }
}
