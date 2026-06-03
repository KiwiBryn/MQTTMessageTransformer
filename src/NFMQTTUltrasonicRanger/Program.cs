// Minimalist ESP32 nanoFramework + Event Grid MQTT broker client example
// Copyright (c) December 2025, devMobile Software
#define CLIENT_WIFI  
#define ESP32_XIAO_ESP32_S3 // Comment out of not using Seeed XIAO ESP32 S3 board SKU 113991114
//#define DEBUG_LOGGER
using System;
using System.IO.Ports;
using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;

using Iot.Device.Modbus.Client;
#if DEBUG_LOGGER
using Microsoft.Extensions.Logging;
using nanoFramework.Logging.Debug;
#endif

#if ESP32_XIAO_ESP32_S3
using nanoFramework.Hardware.Esp32;
#endif

using nanoFramework.Json;
using nanoFramework.M2Mqtt;
using nanoFramework.M2Mqtt.Messages;
using nanoFramework.Networking;
using System.Device.Wifi;

/*
The EdgeBox 100 firmware ESP32_S3_ALL_UART or ESP32_S3_BLE_UART should work for you depending on type of psram it has.

Generate a new EC private key using the prime256v1 curve
   //openssl ecparam -name prime256v1 -genkey -noout -out device.key
   openssl ecparam -name prime256v1 -genkey -noout -out Edgebox100Z.key

Generate a device CSR using the private key
   //openssl req -new -key device.key -out device.csr -subj "/CN=device.example.com/O=YourOrg/OU=IoT"
   openssl req -new -key Edgebox100Z.key -out Edgebox100Z.csr -subj "/CN=Edgebox100Z/O=devMobileSoftware /OU=IoT"

Sign the device CSR with your intermediate cert + key to issue the device certificate:
   //openssl x509 -req -in device.csr -CA IntermediateCA.crt -CAkey IntermediateCA.key -CAcreateserial -out device.crt -days 365 -sha256
   openssl x509 -req -in Edgebox100Z.csr -CA EnvironmentalSensorsCA.crt -CAkey EnvironmentalSensorsCA.key -CAcreateserial -out Edgebox100Z.crt -days 365 -sha256

The PEM encoded root CA certificate chain that is used to validate the server
const char CA_ROOT_PEM[] PROGMEM = R"PEM(
-----BEGIN CERTIFICATE-----
      Thumbprint: 56D955C849887874AA1767810366D90ADF6C8536
      CN: CN = Microsoft Azure ECC TLS Issuing CA 03
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
      Thumbprint: 7E04DE896A3E666D00E687D33FFAD93BE83D349E
      CN: CN = DigiCert Global Root G3
-----END CERTIFICATE-----
)PEM";

The PEM encoded certificate chain that is used to authenticate the device
static const char CLIENT_CERT_PEM[] PROGMEM = R"PEM(
-----BEGIN CERTIFICATE-----
 CN=Self signed device certificate
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
 CN=Self signed Intermediate certificate
-----END CERTIFICATE-----
)PEM";

 The PEM encoded private key of device
static const char CLIENT_KEY_PEM[] PROGMEM = R"PEM(
-----BEGIN EC PRIVATE KEY-----
-----END PRIVATE KEY-----
)PEM";
*/

namespace devMobile.AzureIoT.EventGridClient
{
   public class Program
   {
      private const MqttSslProtocols sslProtocol = MqttSslProtocols.TLSv1_3;
      private const MqttProtocolVersion protocolVersion = MqttProtocolVersion.Version_5;
      private const bool cleanSession = true;
      private const ushort keepAlivePeriod = 240;
      private const string MQTT_TOPIC_PUBLISH_FORMAT = "device/{0}/distance";
      private const string MQTT_TOPIC_SUBSCRIBE_FORMAT = "device/{0}/configuration, device/+/reboot";

      // === Sensor Modbus params (from Seeed datasheet) ===
      private const byte SlaveAddress = 0x01;      // default
      private const ushort RegCalcDistance = 0x0100;// mm, ~500ms processing
      private const ushort RegRealDistance = 0x0101;// mm, ~100ms
      private const ushort RegTemperature = 0x0102;// INT16, 0.1°C units
      private const ushort RegSlaveAddress = 0x0200;// R/W address register

      public static void Main()
      {
         Thread.Sleep(1000); // Found this works around some issues with running immediately after a reset
         Console.WriteLine(".NET nanoFramework MQTT UltrasonicRanger starting...");

#if ESP32_XIAO_ESP32_S3
         Configuration.SetPinFunction(Gpio.IO06, DeviceFunction.COM2_RX);
         Configuration.SetPinFunction(Gpio.IO05, DeviceFunction.COM2_TX);
         Configuration.SetPinFunction(Gpio.IO03, DeviceFunction.COM2_RTS);
#endif
         var ports = SerialPort.GetPortNames();

         Console.WriteLine("Available serial ports: ");
         foreach (string port in ports)
         {
            Console.WriteLine($" {port}");
         }

#if CLIENT_WIFI
         Console.WriteLine("WiFi connecting...");

         while (WifiNetworkHelper.Status != NetworkHelperStatus.NetworkIsReady)
         {
            // Attempt to connect using DHCP
            if (!WifiNetworkHelper.ConnectDhcp(Secrets.WIFI_SSID, Secrets.WIFI_PASSWORD, reconnectionKind: WifiReconnectionKind.Automatic, requiresDateTime: true))
            {
               Console.WriteLine($"Failed to connect. Error: {WifiNetworkHelper.Status}");
               if (WifiNetworkHelper.HelperException != null)
               {
                  Console.WriteLine($"Exception: {WifiNetworkHelper.HelperException}");
               }
               Thread.Sleep(5000);
            }
         }
         Console.WriteLine("WiFi connected");
#endif

         X509Certificate serverCertificate = null;
#if SERVER_CERTIFICATE_VALIDATION
         serverCertificate = new X509Certificate(Constants.CA_ROOT_PEM);
#endif

         X509Certificate2 clientCertificate = null;
#if CLIENT_CERTIFICATE_AUTHENTICATION
         try
         {
            clientCertificate = new X509Certificate2(Secrets.CLIENT_CERT_PEM_A, Secrets.CLIENT_KEY_PEM_A, string.Empty);
         }
         catch (Exception ex)
         {
            Console.WriteLine($"Client Certificate Exception: {ex.Message}");
         }
#endif
         bool secure = serverCertificate is null || clientCertificate is null;

         using (MqttClient mqttClient = new MqttClient(Secrets.MQTT_SERVER))// secure, serverCertificate, clientCertificate, sslProtocol);
         {
            mqttClient.ProtocolVersion = protocolVersion;

            mqttClient.MqttMsgPublishReceived += MqttMsgPublishReceived;
            mqttClient.MqttMsgSubscribed += MqttMsgSubscribed;
            mqttClient.MqttMsgUnsubscribed += MqttMsgUnsubscribed;
            mqttClient.ConnectionOpened += ConnectionOpened;
            mqttClient.ConnectionClosed += ConnectionClosed;
            mqttClient.ConnectionClosedRequest += ConnectionClosedRequest;

            while (!mqttClient.IsConnected)
            {
               Console.WriteLine("MQTT connecting...");

               try
               {
                  var resultConnect = mqttClient.Connect(Secrets.MQTT_CLIENTID, Secrets.MQTT_USERNAME, Secrets.MQTT_PASSWORD);
                  if (resultConnect != MqttReasonCode.Success)
                  {
                     Console.WriteLine($"MQTT ERROR connecting: {resultConnect}");
                     Thread.Sleep(1000);
                  }
               }
               catch (Exception ex)
               {
                  Console.WriteLine($"MQTT ERROR Exception '{ex.Message}'");
                  Thread.Sleep(1000);
               }
            }
            Console.WriteLine("MQTT connected...");


            foreach (string topic in MQTT_TOPIC_SUBSCRIBE_FORMAT.Split(','))
            {
               string topicSubscribe = string.Format(topic, Secrets.MQTT_CLIENTID);

               mqttClient.Subscribe(new[] { topicSubscribe }, new[] { MqttQoSLevel.AtLeastOnce });
            }

            string topicPublish = string.Format(MQTT_TOPIC_PUBLISH_FORMAT, Secrets.MQTT_CLIENTID);

            using (var modbusClient = new ModbusClient("COM2"))
            {
               modbusClient.ReadTimeout = modbusClient.WriteTimeout = 2000;

#if DEBUG_LOGGER
         _client.Logger = new DebugLogger("ModbusClient")
         {
            MinLogLevel = LogLevel.Debug
         };
#endif

               while (true)
               {
                  try
                  {
                     // 0x0100 Calculated distance (mm). Takes ~500ms to compute per datasheet.
                     short[] distanceRaw = modbusClient.ReadHoldingRegisters(SlaveAddress, RegCalcDistance, 1);
                     ushort mm = (ushort)distanceRaw[0];
                     float cm = mm / 10.0f;

                     // 0x0102 Temperature (INT16, 0.1°C)
                     short[] temperatureRaw = modbusClient.ReadHoldingRegisters(SlaveAddress, RegTemperature, 1);
                     float temperatureC = unchecked(temperatureRaw[0]) / 10.0f; // signed per datasheet

                     Console.WriteLine($"Distance: {mm} mm {cm:F1} cm Temperature: {temperatureC:F1} °C");

                     var payload = new MessagePayload() { ClientID = Secrets.MQTT_CLIENTID, Cm = cm, Mm = mm, Temperature = temperatureC };

                     string jsonPayload = JsonSerializer.SerializeObject(payload);

                     try
                     {
                        Console.WriteLine("MQTT publish message start...");

                        var result = mqttClient.Publish(topicPublish, Encoding.UTF8.GetBytes(jsonPayload), "application/json; charset=utf-8", null);

                        Debug.WriteLine($"MQTT published ({result})");
                     }
                     catch (Exception ex)
                     {
                        Console.WriteLine($"MQTT publish failed: {ex.Message}");
                     }
                  }
                  catch (Exception ex)
                  {
                     Console.WriteLine($"Modbus read failed: {ex.Message}");
                  }

                  Thread.Sleep(500);
               }
            }
         }
      }


      private static void MqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e)
      {
         Console.WriteLine($"MqttMsgPublishReceived Topic:{e.Topic}");

         Console.WriteLine(Encoding.UTF8.GetString(e.Message, 0, e.Message.Length));
      }

      private static void MqttMsgSubscribed(object sender, MqttMsgSubscribedEventArgs e)
      {
         Console.WriteLine($"MqttMsgSubscribed Message identifier:{e.MessageId} of subscribed topic");
      }

      private static void MqttMsgUnsubscribed(object sender, MqttMsgUnsubscribedEventArgs e)
      {
         Console.WriteLine("MqttMsgUnsubscribed");
      }

      private static void ConnectionOpened(object sender, ConnectionOpenedEventArgs e)
      {
         Console.WriteLine("ConnectionOpened");
      }

      private static void ConnectionClosed(object sender, EventArgs e)
      {
         Console.WriteLine("ConnectionClosed");
      }

      private static void ConnectionClosedRequest(object sender, ConnectionClosedRequestEventArgs e)
      {
         Console.WriteLine("ConnectionClosedRequest");
      }


      public class MessagePayload
      {
         public string ClientID { get; set; }
         public float Temperature { get; set; }
         public ushort Mm { get; set; }
         public float Cm { get; set; }
      };
   }
}

