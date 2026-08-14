//---------------------------------------------------------------------------------
// Copyright (c) August 2026, devMobile Software
//
using devMobile.IoT.MqttTransFormer.LightGBM.Model;


namespace devMobile.IoT.MqttTransFormer.LightGBM;

/// <summary>
/// Abstraction over a per-topic ML.NET prediction pipeline. One instance owns
/// the loaded model, its PredictionEngine, the paired output transformer, and
/// the lock that serialises access to the engine (PredictionEngine is not
/// thread-safe).
/// </summary>
internal interface ITopicPredictor
{
   byte[] PredictAndTransform(string topic, ModelInput input);
}

/// <summary>
/// Generic strategy that binds a prediction POCO to an output transformer.
/// One closed generic type per <see cref="ModelType"/>.
/// </summary>
internal sealed class TopicPredictor<TPrediction> : ITopicPredictor
   where TPrediction : class, new()
{
   private readonly PredictionEngine<ModelInput, TPrediction> _engine;
   private readonly Func<string, TPrediction, byte[]> _outputTransform;
   private readonly object _gate = new();

   public TopicPredictor(
      PredictionEngine<ModelInput, TPrediction> engine,
      Func<string, TPrediction, byte[]> outputTransform)
   {
      _engine = engine;
      _outputTransform = outputTransform;
   }

   public byte[] PredictAndTransform(string topic, ModelInput input)
   {
      TPrediction prediction;
      lock (_gate)
      {
         prediction = _engine.Predict(input);
      }
      return _outputTransform(topic, prediction);
   }
}

/// <summary>
/// Builds an <see cref="ITopicPredictor"/> for a topic. This is the ONE place
/// where <see cref="ModelType"/> is switched on. Adding a new model type =
/// adding a case here and a new prediction POCO — no other file changes.
/// </summary>
internal static class PredictorFactory
{
   public static ITopicPredictor Create(MLContext mlContext, TopicConfiguration settings)
   {
      if (!File.Exists(settings.ModelFileName))
         throw new FileNotFoundException($"Model file not found: {settings.ModelFileName}");

      using var fs = File.OpenRead(settings.ModelFileName);
      var model = mlContext.Model.Load(fs, out var inputSchema);

      // Bind ModelInput.Features to whatever fixed size the trained model uses.
      var trainedFeatures = (VectorDataViewType)inputSchema["Features"].Type;
      var schemaDef = SchemaDefinition.Create(typeof(ModelInput));
      schemaDef["Features"].ColumnType =
         new VectorDataViewType(NumberDataViewType.Single, trainedFeatures.Size);

      return settings.ModelType switch
      {
         ModelType.Regression => new TopicPredictor<PredictionRegression>(
            mlContext.Model.CreatePredictionEngine<ModelInput, PredictionRegression>(model, inputSchemaDefinition: schemaDef),
            (topic, p) => settings.OutputMessageRegressionTransformer!.Transform(topic, p)),

         ModelType.BinaryClassification => new TopicPredictor<PredictionBinary>(
            mlContext.Model.CreatePredictionEngine<ModelInput, PredictionBinary>(model, inputSchemaDefinition: schemaDef),
            (topic, p) => settings.OutputMessageBinaryTransformer!.Transform(topic, p)),

         ModelType.MultiClassClassification => new TopicPredictor<PredictionMultiClass>(
            mlContext.Model.CreatePredictionEngine<ModelInput, PredictionMultiClass>(model, inputSchemaDefinition: schemaDef),
            (topic, p) => settings.OutputMessageClassificationTransformer!.Transform(topic, p)),

         _ => throw new NotSupportedException($"ModelType '{settings.ModelType}' is not supported.")
      };
   }
}