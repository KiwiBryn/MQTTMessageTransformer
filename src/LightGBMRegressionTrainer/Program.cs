using System;
using Microsoft.ML;
using Microsoft.ML.Data;

public class ModelInput
{
   [LoadColumn(1, 6)]
   [VectorType(6)]
   public float[] Features { get; set; } = [];

   [LoadColumn(0)]
   public float Label { get; set; }
}

public class PredictionRegression
{
   public float Value { get; set; }
}

class Program
{
   static void Main(string[] args)
   {
      Console.WriteLine("Training LightGBM regression model...");

      if (args.Length != 2)
      {
         Console.WriteLine("Usage: LightGBMRegressionTrainer [input.csv] [output_model.zip]");
         return;
      }

      if (!System.IO.File.Exists(args[0]))
      {
         Console.WriteLine($"CSV file not found. Please ensure '{args[0]}' is in the application directory.");
         return;
      }

      var ml = new MLContext(seed: 42);

      // Load CSV
      var data = ml.Data.LoadFromTextFile<ModelInput>(
          path: args[0],
          hasHeader: true,
          separatorChar: ',');

      var split = ml.Data.TrainTestSplit(data, testFraction: 0.2);

      // LightGBM regression trainer
      var pipeline =
          ml.Regression.Trainers.LightGbm(
              labelColumnName: nameof(ModelInput.Label),
              featureColumnName: nameof(ModelInput.Features));

      var model = pipeline.Fit(split.TrainSet);

      // Evaluate
      var predictions = model.Transform(split.TestSet);
      var metrics = ml.Regression.Evaluate(predictions);

      Console.WriteLine($"R²: {metrics.RSquared}");
      Console.WriteLine($"RMSE: {metrics.RootMeanSquaredError}");

      // Save model
      ml.Model.Save(model, split.TrainSet.Schema, args[1]);

      Console.WriteLine("Model training complete. Press <enter> to exit.");
      Console.ReadLine();
   }
}
