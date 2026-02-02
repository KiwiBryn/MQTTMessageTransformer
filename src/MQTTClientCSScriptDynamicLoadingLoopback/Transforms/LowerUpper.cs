using HiveMQtt.MQTT5.Types;
using System.Text;

public class messageTransformer : IMessageTransformer
{
   public MQTT5PublishMessage[] Transform(MQTT5PublishMessage message)
   {
      if (message.Payload is null)
      {
         return [];
      }

      // Example: echo the payload to a new topic
      var payload = Encoding.UTF8.GetString(message.Payload);

      // Simple transformations: convert to both upper and lower case
      var toLower = new MQTT5PublishMessage
      {
         Topic = message.Topic,
         Payload = Encoding.UTF8.GetBytes(payload.ToLower()),
         QoS = QualityOfService.AtLeastOnceDelivery
      };

      var toUpper = new MQTT5PublishMessage
      {
         Topic = message.Topic,
         Payload = Encoding.UTF8.GetBytes(payload.ToUpper()),
         QoS = QualityOfService.AtLeastOnceDelivery
      };

      return [toLower, toUpper];
   }
}
