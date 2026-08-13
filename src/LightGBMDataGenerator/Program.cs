using System;
using System.Collections.Generic;

class Program
{
   static void Main( string[] args )
   {
      if (args.Length != 1 && args[0] == "--help")
      {
         Console.WriteLine("Usage: LightGBMDataGenerator [output.csv]");
         return;
      }

      string outputPath = args.Length > 0 ? args[0] : "output.csv";

      EnvGeneratorFull.GenerateCsv(outputPath);
   }

   public static class GeneratorConfig
   {
      public const int MonthsToGenerate = 6;

      public const float SeasonalTempAmp = 4f;
      public const float SeasonalHumAmp = 5f;
      public const float SeasonalParAmp = 0.2f;
      public const float SeasonalSoilAmp = 10f;   // wetter in winter, drier in summer

      public const float TempWeight = 0.35f;
      public const float HumWeight = 0.25f;
      public const float ParWeight = 0.20f;
      public const float SoilWeight = 0.20f;

      public const float NoiseScale = 0.5f;

      public const int AnomalyCount = 40;
   }


   public static class EnvGeneratorFull
   {
      public static void GenerateCsv(string outputPath)
      {
         DateTime startDate = new DateTime(2025, 01, 01);
         int months = GeneratorConfig.MonthsToGenerate;

         using var writer = new StreamWriter(outputPath);
         writer.WriteLine("Label,Features.0,Features.1,Features.2,Features.3,Features.4,Features.5");

         var rand = new Random();
         var endDate = startDate.AddMonths(months);

         var anomalyTimes = new HashSet<DateTime>();
         for (int i = 0; i < GeneratorConfig.AnomalyCount; i++)
         {
            var offsetMinutes = rand.Next(0, (int)(endDate - startDate).TotalMinutes);
            anomalyTimes.Add(startDate.AddMinutes(offsetMinutes));
         }

         float soilMoisture = 40f; // starting point

         foreach (var ts in EnumerateTimes(startDate, endDate, 5))
         {
            int minutesOfDay = ts.Hour * 60 + ts.Minute;

            float hourNorm = minutesOfDay / 1440f;
            float hourSin = (float)Math.Sin(2 * Math.PI * hourNorm);
            float hourCos = (float)Math.Cos(2 * Math.PI * hourNorm);

            float seasonNorm = (float)(ts - startDate).TotalDays / (months * 30f);
            float seasonSin = (float)Math.Sin(2 * Math.PI * seasonNorm);

            float expectedTempBase = 18f + seasonSin * GeneratorConfig.SeasonalTempAmp;
            float expectedHumBase = 70f - seasonSin * GeneratorConfig.SeasonalHumAmp;
            float expectedParScale = 1f + seasonSin * GeneratorConfig.SeasonalParAmp;
            float expectedSoilBase = 40f + seasonSin * GeneratorConfig.SeasonalSoilAmp;

            float expectedTemp =
                expectedTempBase +
                8f * (float)Math.Sin((2 * Math.PI / 1440) * (minutesOfDay - 480));

            float expectedHum =
                expectedHumBase -
                (expectedTemp - expectedTempBase) * 2f;

            float expectedPar = 0f;
            if (minutesOfDay > 360 && minutesOfDay < 1080)
            {
               float dayPos = (minutesOfDay - 360) / 720f;
               expectedPar = 1500f * (float)Math.Sin(Math.PI * dayPos) * expectedParScale;
            }

            float temp = expectedTemp +
                (float)(rand.NextDouble() * GeneratorConfig.NoiseScale - GeneratorConfig.NoiseScale / 2);

            float humidity = expectedHum +
                (float)(rand.NextDouble() * 2 - 1);

            float par = expectedPar;
            if (par > 0)
            {
               float cloud = 0.7f + (float)rand.NextDouble() * 0.3f;
               par *= cloud;
            }

            bool rainfallEvent = false;

            if (anomalyTimes.Contains(ts))
            {
               int anomalyType = rand.Next(4);

               switch (anomalyType)
               {
                  case 0:
                     temp += (rand.NextDouble() < 0.5 ? -1 : 1) *
                             (6f + (float)rand.NextDouble() * 4f);
                     break;

                  case 1:
                     humidity += 15f + (float)rand.NextDouble() * 10f;
                     rainfallEvent = true;
                     break;

                  case 2:
                     par *= 0.1f + (float)rand.NextDouble() * 0.1f;
                     break;

                  case 3:
                     soilMoisture += 20f + (float)rand.NextDouble() * 10f;
                     rainfallEvent = true;
                     break;
               }
            }

            float evapRate = par > 0 ? par / 1500f * 0.5f : 0.1f;
            float rainfallBoost = rainfallEvent ? 10f + (float)rand.NextDouble() * 10f : 0f;

            soilMoisture =
                soilMoisture
                - evapRate
                + rainfallBoost
                + (expectedSoilBase - soilMoisture) * 0.01f
                + (float)(rand.NextDouble() * 0.5 - 0.25);

            soilMoisture = Math.Clamp(soilMoisture, 0f, 100f);

            float tempSeverity = Math.Abs(temp - expectedTemp);
            float humSeverity = Math.Abs(humidity - expectedHum);
            float parSeverity = Math.Abs(par - expectedPar);
            float soilSeverity = Math.Abs(soilMoisture - expectedSoilBase);

            float evt =
                tempSeverity * GeneratorConfig.TempWeight +
                humSeverity * GeneratorConfig.HumWeight +
                parSeverity * GeneratorConfig.ParWeight +
                soilSeverity * GeneratorConfig.SoilWeight;

            writer.WriteLine($"{evt},{temp},{humidity},{par},{hourSin},{hourCos},{soilMoisture}");
         }
      }

      private static IEnumerable<DateTime> EnumerateTimes(DateTime start, DateTime end, int stepMinutes)
      {
         for (var ts = start; ts < end; ts = ts.AddMinutes(stepMinutes))
            yield return ts;
      }
   }
}
