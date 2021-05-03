package com.project.carCostPrediction;

import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CarMain {
	
	public static void main(String[] args) {
		
		CarsFeatureSelection feature=new CarsFeatureSelection();
		Models model=new Models();
		
		Dataset<Row> csvData=feature.init();
		
		csvData=feature.cleanData(csvData);
		
		csvData=feature.indexing(csvData);
		
		csvData=feature.indexToVector(csvData);
		
		Dataset<Row> modelInput=feature.getModelInput(csvData);
		
		
		Dataset<Row>[] modelData=modelInput.randomSplit(new double[] {0.8,0.2});
		Dataset<Row> tainData=modelData[0];
		Dataset<Row> testData=modelData[1];
		
		LinearRegressionModel lrModel = model.linearRegModel(tainData);
		
		System.out.println("RegParam: "+lrModel.getRegParam());
		System.out.println("ElasticParam: "+lrModel.getElasticNetParam());
		
		lrModel.transform(testData).show();
		System.out.println("Taining  r2 value: "+lrModel.summary().r2()+" , and RMSE: "+lrModel.summary().rootMeanSquaredError());
		System.out.println("Test  r2 value: "+lrModel.evaluate(testData).r2()+" , and RMSE: "+lrModel.evaluate(testData).rootMeanSquaredError());
		
	}

}
