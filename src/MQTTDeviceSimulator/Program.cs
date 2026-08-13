using HiveMQtt.Client;
using HiveMQtt.Client.Options;
using HiveMQtt.MQTT5.ReasonCodes;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Reflection.PortableExecutable;
using System.Text.Json;
using System.Threading.Tasks;


namespace devMobile.IoT.MQTTLightGBMDeviceSimulator;

class Program
{
   private static Model.ApplicationSettings? _applicationSettings;

   private static HiveMQClient? _client;

   static async Task Main(string[] args)
   {
      Console.WriteLine("Starting HiveMQ publisher...");

      try
      {
         // load the app settings into configuration
         var configuration = new ConfigurationBuilder()
              .AddJsonFile("appsettings.json", false, true)
              .AddUserSecrets<Program>()
         .Build();

         _applicationSettings = configuration.GetSection("ApplicationSettings").Get<Model.ApplicationSettings>();
         if (_applicationSettings is null)
         {
            throw new Exception("ApplicationSettings not configured");
         }

         var optionsBuilder = new HiveMQClientOptionsBuilder();

         optionsBuilder
            .WithClientId(_applicationSettings.ClientId)
            .WithBroker(_applicationSettings.Host)
            .WithPort(_applicationSettings.Port)
            .WithUserName(_applicationSettings.UserName)
            .WithCleanStart(_applicationSettings.CleanStart)
            .WithUseTls(_applicationSettings.UseTls);

#if HIVEMQ_CERTIFICATE_SUPPORT 
         if (!string.IsNullOrWhiteSpace(_applicationSettings.ClientCertificateFileName))
         {
            optionsBuilder.WithClientCertificate(_applicationSettings.ClientCertificateFileName, _applicationSettings.ClientCertificatePassword);
         }
#endif

#if HIVEMQ_USERNAME_AND_PASSWORD_SUPPORT
         if (!string.IsNullOrWhiteSpace(_applicationSettings.Password))
         {
            optionsBuilder = optionsBuilder.WithPassword(_applicationSettings.Password);
         }
#endif

         using (_client = new HiveMQClient(optionsBuilder.Build()))
         {
            var connectResult = await _client.ConnectAsync();
            if (connectResult.ReasonCode != ConnAckReasonCode.Success)
            {
               throw new Exception($"Failed to connect: {connectResult.ReasonCode}");
            }

            using var reader = new StreamReader(_applicationSettings.SensorDataFilePath);
            string? line;

            // Skip header
            reader.ReadLine();

            while ((line = reader.ReadLine()) != null)
            {
               string json = ConvertCsvToJson(line);

               await _client.PublishAsync(_applicationSettings.Topic, json);

               Console.WriteLine($"[{_applicationSettings.Topic}] {json}");

               await Task.Delay(500);
            }

            await _client.DisconnectAsync();
            Console.WriteLine("Publishing complete.");
         }
      }
      catch (Exception ex)
      {
         Console.WriteLine($"{DateTime.UtcNow:yy-MM-dd HH:mm:ss} Application startup failure {ex.Message}", ex);
      }

      Console.WriteLine("Press Enter to exit");
      Console.ReadLine();
   }


   static string ConvertCsvToJson(string csvLine)
   {
      var parts = csvLine.Split(',');

      float evt = float.Parse(parts[0]);

      float temp = float.Parse(parts[1]);
      float humidity = float.Parse(parts[2]);
      float par = float.Parse(parts[3]);
      float hourSin = float.Parse(parts[4]);
      float hourCos = float.Parse(parts[5]);
      float soilMoisture = float.Parse(parts[6]);

      var obj = new
      {
//         evt,
         temp,
         humidity,
         par,
         hourSin,
         hourCos,
         soilMoisture
      };

      return JsonSerializer.Serialize(obj);
   }
}
