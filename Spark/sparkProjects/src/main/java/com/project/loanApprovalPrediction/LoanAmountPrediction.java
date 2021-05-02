package com.project.loanApprovalPrediction;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

import java.util.Arrays;
import java.util.List;

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
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoanAmountPrediction {
	public static Dataset<Row> init() {
		System.setProperty("hadoop.home.dir", "C:/Users/Rakesh/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark=SparkSession.builder().appName("Loan Amount prediction")
				.master("local[*]").config("spark.some.config.option", "some-value")
				.getOrCreate();
		Dataset<Row> input=spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/input/loan_approval.csv");
		
		return input;
	}
	public static Dataset cleanData(Dataset csvData) {
		csvData=csvData.withColumn("Credit_History", when(col("Credit_History").isNull(),0).otherwise(col("Credit_History")))
		.withColumn("Loan_Status", when(col("Loan_Status").like("Y"),1).otherwise(0))
		.withColumn("ApplicantIncome", when(col("ApplicantIncome").isNull(),0).otherwise(col("ApplicantIncome")))
		.withColumn("CoapplicantIncome", when(col("CoapplicantIncome").isNull(),0).otherwise(col("CoapplicantIncome")))
        .withColumn("LoanAmount", when(col("LoanAmount").isNull(),0).otherwise(col("LoanAmount")))
        .withColumn("Loan_Amount_Term", when(col("Loan_Amount_Term").isNull(),0).otherwise(col("Loan_Amount_Term")));
		return csvData;
	}
	public static Dataset stringToInteger(Dataset csvData) {



		StringIndexer GenderIndex=new StringIndexer()
				.setInputCol("Gender")
				.setOutputCol("GenderIndex")
				.setHandleInvalid("keep");
		csvData=GenderIndex.fit(csvData).transform(csvData);
		
		StringIndexer Self_EmployedIndex=new StringIndexer()
				.setInputCol("Self_Employed")
				.setOutputCol("Self_EmployedIndex")
				.setHandleInvalid("keep");
		csvData=Self_EmployedIndex.fit(csvData).transform(csvData);
		
		StringIndexer MarriedIndex=new StringIndexer()
				.setInputCol("Married")
				.setOutputCol("MarriedIndex")
				.setHandleInvalid("keep");
		csvData=MarriedIndex.fit(csvData).transform(csvData);
		
		StringIndexer EducationIndex=new StringIndexer()
				.setInputCol("Education")
				.setOutputCol("EducationIndex")
				.setHandleInvalid("keep");
		csvData=EducationIndex.fit(csvData).transform(csvData);
		
		StringIndexer Property_AreaIndex=new StringIndexer()
				.setInputCol("Property_Area")
				.setOutputCol("Property_AreaIndex")
				.setHandleInvalid("keep");
		csvData=Property_AreaIndex.fit(csvData).transform(csvData);
		
		StringIndexer DependentsIndex=new StringIndexer()
				.setInputCol("Dependents")
				.setOutputCol("DependentsIndex")
				.setHandleInvalid("keep");
		csvData=DependentsIndex.fit(csvData).transform(csvData);
     return csvData;
	}
	public static Dataset integerToVector(Dataset csvData) {
		
		OneHotEncoder encoder=new OneHotEncoder()
				.setInputCols(new String[] {"GenderIndex","Self_EmployedIndex","MarriedIndex","EducationIndex","Property_AreaIndex","DependentsIndex"})
				.setOutputCols(new String[] {"GenderVector","Self_EmployedVector","MarriedVector","EducationVector","Property_AreaVector","DependentsVector"});
		csvData=encoder.fit(csvData).transform(csvData);
		return csvData;

	}
	public static void trainedAndTestLogisticModel(Dataset holdOutData,LogisticRegressionModel lrModel) {
		
		
//		modelInput.show();
//		csvData.describe().show();
		
		System.out.println("Training data accuracy : "+lrModel.summary().accuracy());

		System.out.println("Coficients: "+lrModel.coefficients()+" Intercept: "+lrModel.intercept());
		
		LogisticRegressionSummary summary=lrModel.evaluate(holdOutData);
		double truePositive=summary.truePositiveRateByLabel()[1];
		double falsePositive=summary.falsePositiveRateByLabel()[0];
		
		System.out.println("For the Hold out data, the likelihood of positive being correct is: "+truePositive/(truePositive+falsePositive));
		System.out.println("The  hold out data accuracy: "+summary.accuracy());
		lrModel.transform(holdOutData).groupBy("label","prediction").count().show();

		

	}
	private static LogisticRegressionModel logisticModel(Dataset<Row> modelInput) {
		
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
			return lrModel;
		
			
		}


	
	

	public static void loanAmountPredict() {
		
		
		Dataset<Row> inputData=init();
		
		
		inputData=cleanData(inputData);

		
		inputData=stringToInteger(inputData);
		
		inputData=integerToVector(inputData);
		
		VectorAssembler assembler=new VectorAssembler()
				.setInputCols(new String[] {"GenderVector","MarriedVector","EducationVector","Self_EmployedVector","ApplicantIncome","CoapplicantIncome","Loan_Amount_Term","Credit_History","Property_AreaVector","LoanAmount"})
				.setOutputCol("features");
		
		Dataset<Row> modelInput=assembler.transform(inputData)
				.select("Loan_Status","features")
				.withColumnRenamed("Loan_Status", "label");

			
		Dataset<Row>[] splitData=modelInput.randomSplit(new double[] {0.8,0.2});
		Dataset<Row> traningData=splitData[0];
		Dataset<Row>  holdOutData=splitData[1];
		
		LogisticRegressionModel model=logisticModel(traningData);
		
		trainedAndTestLogisticModel(holdOutData,model);	
	}
	
	public static void main(String[] args) {
	
	
		loanAmountPredict();
	}

}