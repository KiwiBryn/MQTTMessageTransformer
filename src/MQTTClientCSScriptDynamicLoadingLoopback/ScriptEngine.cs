//---------------------------------------------------------------------------------
// Copyright (c) February 2026, devMobile Software
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// https://github.com/hivemq/hivemq-mqtt-client-dotnet 
//
namespace devMobile.IoT.MqttTransformer.CSScriptDynamicLoadingLoopback;

public class ScriptEngine(IMemoryCache cache, string scriptPath)
{
   private readonly IMemoryCache _cache = cache;
   private readonly string _scriptPath = scriptPath;

    public IMessageTransformer? GetTransformer()
   {
      return _cache.GetOrCreate("transformer-script", entry =>
      {
         entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(10);
         entry.SlidingExpiration = TimeSpan.FromMinutes(2);

         try
         {
            var evaluator = CSScript.Evaluator.LoadFile<IMessageTransformer>(_scriptPath);

            return evaluator;
         }
         catch (Exception ex)
         {
            Console.WriteLine($"CSScript.EvaluatorConfig.Engine Exception {ex.Message}");

            throw;
         }
      });
   }
}
