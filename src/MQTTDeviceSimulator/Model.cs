//---------------------------------------------------------------------------------
// Copyright (c) May 2026, devMobile Software
//
namespace devMobile.IoT.MQTTLightGBMDeviceSimulator.Model
{

   internal class ApplicationSettings
   {
      public required string ClientId { get; set; } = string.Empty;
      public required string Host { get; set; } = string.Empty;
      public int Port { get; set; } = 8883;
      public bool CleanStart { get; set; } = true;
      public bool UseTls { get; set; } = true;
      public bool AutomaticReconnect { get; set; } = true;
      public required string Topic { get; set; } = string.Empty;
      public required string SensorDataFilePath { get; set; } = string.Empty;

#if HIVEMQ_USERNAME_AND_PASSWORD_SUPPORT      
      public required string UserName { get; set; } = string.Empty;
      public required string Password { get; set; } = string.Empty;
#endif

#if HIVEMQ_CERTIFICATE_SUPPORT
      public required string ClientCertificateFileName { get; set; } = string.Empty;
      public required string ClientCertificatePassword { get; set; } = string.Empty;
#endif
   }
}