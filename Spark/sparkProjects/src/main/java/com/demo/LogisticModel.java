package com.demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionSummary;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.project.loanApprovalPrediction.LoanAmountPrediction;

import static org.apache.spark.sql.functions.*;

public class LogisticModel {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark=SparkSession.builder()
				.appName("Loan Approval Prediction")
				.master("local[*]")
				.config("spark.some.config.option","some-value")
				.getOrCreate();
		Dataset<Row> csvData=spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/input/loan_prediction.csv");
		csvData=csvData.drop("Loan_ID");
		// male=1,self_employed_Yes=1,Married_Yes=1,Education_Graduation=1
		csvData=csvData.withColumn("Gender", when(col("Gender").like("Female"),0).otherwise(1))
				.withColumn("Self_Employed", when(col("Self_Employed").like("No"),0).otherwise(1))
				.withColumn("Married", when(col("Married").like("No"),0).otherwise(1))
				.withColumn("Education", when(col("Education").like("Not Graduate"),0).otherwise(1))
				.withColumn("Dependents", when(col("Dependents").isNull(),0).otherwise(1))
				.withColumn("Credit_History", when(col("Credit_History").isNull(),0).otherwise(1))
				.withColumn("Property_Area", when(col("Property_Area").like("Urban"),1).otherwise(0))
				.withColumn("ApplicantIncome", when(col("ApplicantIncome").$greater$eq(43517),1).otherwise(0))
				.withColumn("CoapplicantIncome", when(col("CoapplicantIncome").$greater$eq(14400),1).otherwise(0))
		        .withColumn("LoanAmount", when(col("LoanAmount").$less$eq(150),1).otherwise(0))
		        .withColumn("Loan_Amount_Term", when(col("Loan_Amount_Term").$greater$eq(150),1).otherwise(0));
////		csvData.show();	
//		LoanAmountPrediction.checkCorrelation(csvData);
		VectorAssembler assembler=new VectorAssembler()
				.setInputCols(new String[] {"Gender","Married","Education","Self_Employed","Dependents","ApplicantIncome","CoapplicantIncome","Loan_Amount_Term","Credit_History","Property_Area"})
				.setOutputCol("features");
		
		Dataset<Row> modelInput=assembler.transform(csvData)
				.select("LoanAmount","features")
				.withColumnRenamed("LoanAmount", "label");
			
		Dataset<Row>[] splitData=modelInput.randomSplit(new double[] {0.8,0.2});
		Dataset<Row> traningAndTestData=splitData[0];
		Dataset<Row>  holdOutData=splitData[1];
		
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
				.setTrainRatio(0.8);
				
				
		TrainValidationSplitModel model=tvs.fit(modelInput);
		
		LogisticRegressionModel lrModel=(LogisticRegressionModel) model.bestModel();
//		csvData.printSchema();
//		modelInput.show();
//		csvData.describe().show();
		System.out.println("Accouracy: "+lrModel.summary().accuracy());
		System.out.println("Coficients: "+lrModel.coefficients()+" Intercept: "+lrModel.intercept());
		
		LogisticRegressionSummary summary=lrModel.evaluate(holdOutData);
		double truePositive=summary.truePositiveRateByLabel()[1];
		double falsePositive=summary.falsePositiveRateByLabel()[0];
		
		System.out.println("For the Hold out data, the likelihood of positive being correct is: "+truePositive/(truePositive+falsePositive));
		System.out.println("The  hold out data accuracy: "+summary.accuracy());
		lrModel.transform(holdOutData).groupBy("label","prediction").count().show();
		
		spark.close();


	}

}
