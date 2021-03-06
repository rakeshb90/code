package com.project.carCostPrediction;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Models {
	public LinearRegressionModel linearRegModel(Dataset<Row> inputData) {
		LinearRegression linearReg=new LinearRegression();
		ParamGridBuilder gridBuilder=new ParamGridBuilder();
		
		ParamMap[] paramMap=gridBuilder
				.addGrid(linearReg.regParam(),new double[] {0,0.5,1} )
				.addGrid(linearReg.elasticNetParam(),new double[] {0,0.5,1} )
				.build();
		
		TrainValidationSplit tvs=new TrainValidationSplit()
				.setEstimator(linearReg)
				.setEvaluator(new RegressionEvaluator().setMetricName("r2"))
				.setEstimatorParamMaps(paramMap)
				.setTrainRatio(0.9);
		
		TrainValidationSplitModel tvModel=tvs.fit(inputData);		
		LinearRegressionModel model=(LinearRegressionModel) tvModel.bestModel();
		return model;
	}

}
