package com.project.loanApprovalPrediction;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tree.DecisionTreeModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class Models {
	  
         // Linear Regression model
		public LinearRegressionModel linearRegModel(Dataset<Row> modelInput) {
			LinearRegression linearRegression=new LinearRegression();
			ParamGridBuilder gridBuilder=new ParamGridBuilder();
			
			ParamMap[] paramMap=gridBuilder
					.addGrid(linearRegression.regParam(),new double[] {0.01,0.1,0.5})
					.addGrid(linearRegression.elasticNetParam(),new double[] {0,0.5,1})
					.build();
			
			TrainValidationSplit tvs=new TrainValidationSplit()
					.setEstimator(linearRegression)
					.setEvaluator(new RegressionEvaluator().setMetricName("r2"))
					.setEstimatorParamMaps(paramMap)
					.setTrainRatio(0.9);
					
					
			TrainValidationSplitModel model=tvs.fit(modelInput);
			
//			LinearRegressionModel lrModel=(LinearRegressionModel) model.bestModel();
			
			LinearRegressionModel lrModel=linearRegression.fit(modelInput);
			return lrModel;
		}
		
		
		// Decision tree regression model 
		public DecisionTreeRegressionModel getDecisionTreeRegModel(Dataset<Row> modelInput) {
			DecisionTreeRegressor dtRegressor=new DecisionTreeRegressor();
			dtRegressor.setMaxDepth(3);
			DecisionTreeRegressionModel model=dtRegressor.fit(modelInput);
			return model;
		}
		
		
		
		// Decision tree classification model
		public DecisionTreeClassificationModel getDecisionTreeClassificationModel(Dataset<Row> modelInput) {
			
			DecisionTreeClassifier dtClassifier=new DecisionTreeClassifier();
			dtClassifier.setMaxDepth(3);
			DecisionTreeClassificationModel model=dtClassifier.fit(modelInput);
			
			return model;
		
		}
		
		
		// Random forest classification model
		public static RandomForestClassificationModel randomForestClassification(Dataset<Row> trainingData) {
			// Random Forest Classification Model
			RandomForestClassifier rfClassifier=new RandomForestClassifier();
			rfClassifier.setMaxDepth(3);
			RandomForestClassificationModel rfModel=rfClassifier.fit(trainingData);		
					
		    return rfModel;
		}
		
		// Logistic regression model
		public static LogisticRegressionModel logisticModel(Dataset<Row> modelInput) {
			
			LogisticRegression logisticRegression=new LogisticRegression();
				ParamGridBuilder gridBuilder=new ParamGridBuilder();
				
				ParamMap[] paramMap=gridBuilder
						.addGrid(logisticRegression.regParam(),new double[] {0.01,0.1,0.5})
						.addGrid(logisticRegression.elasticNetParam(),new double[] {0,0.5,1})
						.build();
				
				TrainValidationSplit tvs=new TrainValidationSplit()
						.setEstimator(logisticRegression)
						.setEvaluator(new RegressionEvaluator().setMetricName("r2"))
						.setEstimatorParamMaps(paramMap)
						.setTrainRatio(0.9);
						
						
				TrainValidationSplitModel model=tvs.fit(modelInput);
				
				LogisticRegressionModel lrModel=(LogisticRegressionModel) model.bestModel();
				return lrModel;
			
				
			}



}
