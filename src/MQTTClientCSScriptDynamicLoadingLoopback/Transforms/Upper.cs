using HiveMQtt.MQTT5.Types;
using System.Text;

public class upperPayloadTransformer : IMessageTransformer
{
   public MQTT5PublishMessage[] Transform(MQTT5PublishMessage message)
   {
      if (message.Payload is null)
      {
         return [];
      }

      // Example: echo the payload to a new topic
      var payload = Encoding.UTF8.GetString(message.Payload);

      // Simple transformations: convert to upper  case
      var toUpper = new MQTT5PublishMessage
      {
         Topic = message.Topic,
         Payload = Encoding.UTF8.GetBytes(payload.ToUpper()),
         QoS = QualityOfService.AtLeastOnceDelivery
      };

      return [toUpper];
   }
}
