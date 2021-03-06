package com.project.loanApprovalPrediction;

import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionSummary;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ModelDriver {
	
	public static void linearRegResult(LinearRegressionModel linearReg,Dataset<Row>testData) {
		
		System.out.println("Training r2 :"+linearReg.summary().r2()+" , and RMSE: "+linearReg.summary().rootMeanSquaredError());
		System.out.println("Testing r2 :"+linearReg.evaluate(testData).r2()+" , and RMSE: "+linearReg.evaluate(testData).rootMeanSquaredError());
	    linearReg.transform(testData).show();
		
	}
	
	public static void logisticRegResult(LogisticRegressionModel logisticReg,Dataset<Row> testData) {
		System.out.println("Model Accuracy: "+logisticReg.summary().accuracy());
		System.out.println("Test Accuracy: "+logisticReg.evaluate(testData).accuracy());
		LogisticRegressionSummary summary = logisticReg.evaluate(testData);
		double truePositive = summary.truePositiveRateByLabel()[1];
		double falsePositive = summary.falsePositiveRateByLabel()[0];
		
		System.out.println("Positive likellyhood: "+truePositive/(truePositive+falsePositive));
		Dataset<Row> predictions = summary.predictions().groupBy("label","prediction").count();
		predictions.show();
		logisticReg.transform(testData).show();
		
	}
	public static void decisionTreeRegResult(DecisionTreeRegressionModel model,Dataset<Row> testData) {
         Dataset<Row> predictionData=model.transform(testData);
		
		MulticlassClassificationEvaluator evaluator=new MulticlassClassificationEvaluator();
		evaluator.setMetricName("accuracy");
		
		System.out.println(model.toDebugString().toString());
		System.out.println("The Accuracy of Model is "+evaluator.evaluate(predictionData));
		
//		modelInput.show();
		model.transform(testData).show();

		
	}
	public static void decisionTreeClassificationResult(DecisionTreeClassificationModel model,Dataset<Row> testData) {
        Dataset<Row> predictionData=model.transform(testData);
		
		MulticlassClassificationEvaluator evaluator=new MulticlassClassificationEvaluator();
		evaluator.setMetricName("accuracy");
		
		System.out.println(model.toDebugString().toString());
		System.out.println("The Accuracy of Model is "+evaluator.evaluate(predictionData));
		
//		modelInput.show();
		model.transform(testData).show();

		
	}
	public static void randomForestResult(RandomForestClassificationModel model,Dataset<Row> testData) {
        Dataset<Row> predictionData=model.transform(testData);
		
		MulticlassClassificationEvaluator evaluator=new MulticlassClassificationEvaluator();
		evaluator.setMetricName("accuracy");
		
		System.out.println(model.toDebugString().toString());
		System.out.println("The Accuracy of Model is "+evaluator.evaluate(predictionData));
		
//		modelInput.show();
		model.transform(testData).show();

		
	}
	


	public static void main(String[] args) {
	ModelFieldSelection feature=new ModelFieldSelection();
	Models models=new Models();
	
	Dataset<Row> csvData=feature.init();
	
	csvData=feature.cleanData(csvData);
	
	csvData=feature.stringIndexing(csvData);
	
	csvData=feature.indexToVector(csvData);
	
//	Dataset<Row> modelInput=feature.getModelInput(csvData);
	Dataset<Row> modelInput=feature.getModelInputForLinearReg(csvData);
	modelInput.show();
	
	Dataset<Row>[] modelData=modelInput.randomSplit(new double[] {0.8,0.2});
	
	Dataset<Row>trainData=modelData[0];
	Dataset<Row>testData=modelData[1];
	
	
	// Result from model
	LinearRegressionModel linearRegModel = models.linearRegModel(trainData);
	linearRegResult(linearRegModel,testData);// change label of model
	
	
//	LogisticRegressionModel logisticRegModel = models.logisticModel(trainData);
//	logisticRegResult(logisticRegModel,testData);
	
	
//	DecisionTreeRegressionModel dtRegModel = models.getDecisionTreeRegModel(trainData);
//	decisionTreeRegResult(dtRegModel,testData);
	
	
//	DecisionTreeClassificationModel dtClassificationModel = models.getDecisionTreeClassificationModel(trainData);
//	decisionTreeClassificationResult(dtClassificationModel,testData);
	

	
//	RandomForestClassificationModel randomForestModel = models.randomForestClassification(trainData);
//	randomForestResult(randomForestModel,testData);
	

	}

}
