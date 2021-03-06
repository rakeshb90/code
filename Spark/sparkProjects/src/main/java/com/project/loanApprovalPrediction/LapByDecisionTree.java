package com.project.loanApprovalPrediction;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
public class LapByDecisionTree {
	public static Dataset init() {
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
		csvData=csvData.na()
				.fill(csvData.groupBy("Gender").count().orderBy(desc("Gender")).first().get(0).toString(),new String[] {"Gender"}).na()
				.fill(csvData.groupBy("Loan_Status").count().orderBy(desc("Loan_Status")).first().get(0).toString(),new String[] {"Loan_Status"}).na()
				.fill(csvData.groupBy("Self_Employed").count().orderBy(desc("Self_Employed")).first().get(0).toString(),new String[] {"Self_Employed"}).na()
				.fill(csvData.groupBy("Married").count().orderBy(desc("Married")).first().get(0).toString(),new String[] {"Married"}).na()
				.fill(csvData.groupBy("Education").count().orderBy(desc("Education")).first().get(0).toString(),new String[] {"Education"}).na()
				.fill(csvData.groupBy("Property_Area").count().orderBy(desc("Property_Area")).first().get(0).toString(),new String[] {"Property_Area"}).na()
				.fill(csvData.groupBy("Dependents").count().orderBy(desc("Dependents")).first().get(0).toString(),new String[] {"Dependents"}).na()
				.fill((double)(int) csvData.groupBy("Credit_History").count().orderBy(desc("Credit_History")).first().get(0),new String[] {"Credit_History"}).na()
			    .fill(csvData.stat().approxQuantile("Loan_Amount_Term", new double[] {0.5}, 0.000000005)[0],new String[] {"Loan_Amount_Term"}).na()
			    .fill(csvData.stat().approxQuantile("LoanAmount", new double[] {0.5}, 0.000000005)[0],new String[] {"LoanAmount"});
	

				return csvData;
	}
	public static Dataset stringToInteger(Dataset csvData) {

		 csvData=csvData.drop("Loan_ID");
			StringIndexer dataIndex=new StringIndexer()
					.setInputCols(new String[] {"Loan_Status","Gender","Self_Employed","Married","Education","Property_Area","Dependents"})
					.setOutputCols(new String[] {"Loan_StatusIndex","GenderIndex","Self_EmployedIndex","MarriedIndex","EducationIndex","Property_AreaIndex","DependentsIndex"})
					.setHandleInvalid("keep");
			csvData=dataIndex.fit(csvData).transform(csvData);

     return csvData;
	}
	public static DecisionTreeRegressionModel decisonTreeRegression(Dataset<Row> trainingData) {
		// Decision Tree Regression Model
				DecisionTreeRegressor dtRegressor=new DecisionTreeRegressor();
				dtRegressor.setMaxDepth(3);
				DecisionTreeRegressionModel model=dtRegressor.fit(trainingData);
				return model;
	}
	public static DecisionTreeClassificationModel decisonTreeClassification(Dataset<Row> trainingData) {
		// Decision Tree Classification Model
				DecisionTreeClassifier dtClassifier=new DecisionTreeClassifier();
				dtClassifier.setMaxDepth(3);
				DecisionTreeClassificationModel model=dtClassifier.fit(trainingData);
				
				return model;
	}
	public static RandomForestClassificationModel randomForestClassification(Dataset<Row> trainingData) {
		// Random Forest Classification Model
		RandomForestClassifier rfClassifier=new RandomForestClassifier();
		rfClassifier.setMaxDepth(3);
		RandomForestClassificationModel rfModel=rfClassifier.fit(trainingData);		
				
	    return rfModel;
	}
	public static void main(String[] args) {
		Dataset<Row> inputData=init();
		
		inputData=cleanData(inputData);
		
		inputData=stringToInteger(inputData);
		VectorAssembler assembler=new VectorAssembler()
				.setInputCols(new String[] {"GenderIndex","MarriedIndex","EducationIndex","Self_EmployedIndex","ApplicantIncome","CoapplicantIncome","Loan_Amount_Term","Credit_History","Property_AreaIndex","LoanAmount"})
				.setOutputCol("features");
		
		
		inputData=inputData.withColumn("Loan_Status", when(col("Loan_Status").like("Y"),1).otherwise(0));
		  
		Dataset<Row> modelInput=assembler.transform(inputData)
				.select("Loan_Status","features")
				.withColumnRenamed("Loan_Status", "label");
		 
		Dataset<Row>[] trainingAndHoldOutData=modelInput.randomSplit(new double[] {0.8,0.2});
		Dataset<Row> trainingData=trainingAndHoldOutData[0];
		Dataset<Row> holdOutData=trainingAndHoldOutData[1];
		
		
		
//		DecisionTreeRegressionModel model = decisonTreeRegression(trainingData);
		DecisionTreeClassificationModel model = decisonTreeClassification(trainingData);
//		RandomForestClassificationModel model = randomForestClassification(trainingData);
		
		Dataset<Row> predictionData=model.transform(holdOutData);
		
		MulticlassClassificationEvaluator evaluator=new MulticlassClassificationEvaluator();
		evaluator.setMetricName("accuracy");
		
		System.out.println(model.toDebugString().toString());
		System.out.println("The Accuracy of Model is "+evaluator.evaluate(predictionData));
		
//		modelInput.show();
		model.transform(trainingData).show();
	}

}
