package com.project.housepricepridiction;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Driver {
     
	public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/Users/Rakesh/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark=SparkSession.builder().appName("House price prediction")
				.master("local[*]").config("spark.some.config.option", "some-value")
				.getOrCreate();
		Dataset<Row> input=spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/input/kc_house_data.csv");
		System.out.println("**************House Price prediction project**************");
//		input.printSchema();
//		input.show();
//		input=input.withColumn("sqrt_above_percentage", col("sqrt_above").devide(col("sqrt_living")));
		
		// considering non-numeric data in our model 
		StringIndexer conditionIndex=new StringIndexer()
				   .setInputCol("condition")
				   .setOutputCol("conditionIndex");
				  input=conditionIndex.fit(input).transform(input);
				  
		   StringIndexer gardeIndex=new StringIndexer()
				   .setInputCol("grade")
				   .setOutputCol("gradeIndex");
		   input=gardeIndex.fit(input).transform(input);
		   
		   StringIndexer zipcodeIndex=new StringIndexer()
				   .setInputCol("zipcode")
				   .setOutputCol("zipcodeIndex");
		   input=zipcodeIndex.fit(input).transform(input);
		   
		   StringIndexer waterfrontIndex=new StringIndexer()
				   .setInputCol("waterfront")
				   .setOutputCol("waterfrontIndex");
		   input=waterfrontIndex.fit(input).transform(input);
		   
		   OneHotEncoder encoder=new OneHotEncoder()
				   .setInputCols(new String[] {"conditionIndex","gradeIndex","zipcodeIndex","waterfrontIndex"})
				   .setOutputCols(new String[] {"conditionVector","gradeVector","zipcodeVector","waterfrontVector"});
	       input=encoder.fit(input).transform(input);
		  
//		   input.show();
		
		
		VectorAssembler vectorAssembler=new VectorAssembler()
				.setInputCols(new String[] {"bathrooms","bedrooms","sqft_living","sqft_above","floors","conditionVector","gradeVector","zipcodeVector","waterfrontVector"})
				.setOutputCol("features");
		Dataset<Row> modelInput=vectorAssembler.transform(input)
				.select("price","features")
				.withColumnRenamed("price","label");
		
	   
	   
//		modelInput.show();

		
		Dataset<Row>[] dataSplit=modelInput.randomSplit(new double[] {0.8,0.2});
		Dataset<Row> trainingAndTestData=dataSplit[0];
		Dataset<Row> holdOutData=dataSplit[1];
		
		LinearRegression linearRegression=new LinearRegression();
		
		// making a matrix for model fitting parameter
		ParamGridBuilder gridBuilder=new ParamGridBuilder();
		ParamMap[] paramMap=gridBuilder.addGrid(linearRegression.regParam(),new double[] {0.01,0.1,0.5})
				.addGrid(linearRegression.elasticNetParam(),new double[] {0,0.5,1})
				.build();
			
		// Spliting data set 
		TrainValidationSplit tainValidationSplit=new TrainValidationSplit()
				.setEstimator(linearRegression)
				.setEvaluator(new RegressionEvaluator().setMetricName("r2"))
				.setEstimatorParamMaps(paramMap)
				.setTrainRatio(0.8);
		TrainValidationSplitModel Model=tainValidationSplit.fit(trainingAndTestData);
		LinearRegressionModel lrModel=(LinearRegressionModel) Model.bestModel();
				
		System.out.println("Training => R2: "+lrModel.summary().r2()+" and RMSE: "+lrModel.summary().rootMeanSquaredError());
		System.out.println("Testing => R2: "+lrModel.evaluate(holdOutData).r2()+" and RMSE: "+lrModel.evaluate(holdOutData).rootMeanSquaredError());
        System.out.println("Cofficients: "+lrModel.coefficients()+" intercept: "+lrModel.intercept());
        System.out.println("Reg Param: "+lrModel.getRegParam()+" Elastic net Param: "+lrModel.getElasticNetParam());
		lrModel.transform(holdOutData).show();
		
	}

}
