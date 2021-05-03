package com.project.loanApprovalPrediction;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.when;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LapKmeanClustering {
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
public static Dataset integerToVector(Dataset csvData) {
		
		OneHotEncoder encoder=new OneHotEncoder()
				.setInputCols(new String[] {"GenderIndex","Self_EmployedIndex","MarriedIndex","EducationIndex","Property_AreaIndex","DependentsIndex"})
				.setOutputCols(new String[] {"GenderVector","Self_EmployedVector","MarriedVector","EducationVector","Property_AreaVector","DependentsVector"});
		csvData=encoder.fit(csvData).transform(csvData);
		return csvData;

	}

	public static void main(String[] args) {
		Dataset<Row> inputData=init();
		
		inputData=cleanData(inputData);
		
		inputData=stringToInteger(inputData);
		
		inputData=integerToVector(inputData);
		
		VectorAssembler assembler=new VectorAssembler()
				.setInputCols(new String[] {"GenderVector","MarriedVector","EducationVector","Self_EmployedVector","ApplicantIncome","CoapplicantIncome","Loan_Amount_Term","Credit_History","Property_AreaVector","LoanAmount"})
				.setOutputCol("features");
		Dataset<Row> modelInput=assembler.transform(inputData).select("features");
		
		KMeans kMean=new KMeans();
		for(int noOfCluster=2;noOfCluster<=8;noOfCluster++) {
		kMean.setK(noOfCluster);
		System.out.println("Number of cluster: "+noOfCluster);
		KMeansModel model=kMean.fit(modelInput);
		Dataset<Row> predictions=model.transform(modelInput);
		predictions.show();
		
		Vector[] clusterCenters=model.clusterCenters();
		for(Vector v:clusterCenters)
			System.out.println(v);
		predictions.groupBy("prediction").count().show();
//		System.out.println("SSE is: "+((Object) model).computeCost(modelInput));
		
		ClusteringEvaluator evaluator=new ClusteringEvaluator();
		System.out.println("Sqaure uqiliate distance "+evaluator.evaluate(predictions));
		
		}
		// take look at  K=5
		
		
		
		modelInput.show();
	}

}
