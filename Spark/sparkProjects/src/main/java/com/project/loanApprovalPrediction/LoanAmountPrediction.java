package com.project.loanApprovalPrediction;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
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
		SparkSession spark=SparkSession.builder().appName("Loan Approval prediction")
				.master("local[*]").config("spark.some.config.option", "some-value")
				.getOrCreate();
		Dataset<Row> input=spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/input/loan_approval.csv");
		
		return input;
	}
	
	public static Dataset cleanInputData(Dataset csvData) {
		csvData=csvData.na()
				.fill(csvData.groupBy("Loan_Status").count().orderBy(desc("Loan_Status")).first().get(0).toString(),new String[] {"Loan_Status"}).na()
				.fill(csvData.groupBy("Gender").count().orderBy(desc("Gender")).first().get(0).toString(),new String[] {"Gender"}).na()
				.fill(csvData.groupBy("Married").count().orderBy(desc("Married")).first().get(0).toString(),new String[] {"Married"}).na()
				.fill(csvData.groupBy("Dependents").count().orderBy(desc("Dependents")).first().get(0).toString(),new String[] {"Dependents"}).na()
				.fill(csvData.groupBy("Self_Employed").count().orderBy(desc("Self_Employed")).first().get(0).toString(),new String[] {"Self_Employed"}).na()
				.fill((double)(int)csvData.groupBy("Credit_History").count().orderBy(desc("Credit_History")).first().get(0),new String[] {"Credit_History"}).na()
				.fill(csvData.stat().approxQuantile("LoanAmount",new double[] {0.5}, 0.000000000005)[0],new String[] {"LoanAmount"}).na()
				.fill(csvData.stat().approxQuantile("Loan_Amount_Term",new double[] {0.5}, 0.000000000005)[0],new String[] {"Loan_Amount_Term"});
			
		return csvData;
	}
	
	
	
	
	public static Dataset stringToInteger(Dataset csvData) {
       csvData=csvData.drop("Loan_ID");
		StringIndexer dataIndex=new StringIndexer()
				.setInputCols(new String[] {"Gender","Self_Employed","Married","Education","Property_Area","Dependents"})
				.setOutputCols(new String[] {"GenderIndex","Self_EmployedIndex","MarriedIndex","EducationIndex","Property_AreaIndex","DependentsIndex"})
				.setHandleInvalid("keep");
		csvData=dataIndex.fit(csvData).transform(csvData);

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
					.setTrainRatio(0.9);
					
					
			TrainValidationSplitModel model=tvs.fit(modelInput);
			
			LogisticRegressionModel lrModel=(LogisticRegressionModel) model.bestModel();
			return lrModel;
		
			
		}


	
	

	public static void loanAmountPredict() {
		
		
		Dataset<Row> inputData=init();
		
		
		inputData=cleanInputData(inputData);


		
		inputData=stringToInteger(inputData);
		
		inputData=integerToVector(inputData);
		
		inputData=inputData.withColumn("Loan_Status", when(col("Loan_Status").like("Y"),1).otherwise(0));
		
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
