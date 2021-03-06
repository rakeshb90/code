package com.project.loanApprovalPrediction;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionSummary;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class Loan {
	
	 
	 
	 public static Seq<String> convertListToSeq(List<String> inputList) {
	        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	    }
	 
	 public static Seq<Column> convertListToSe(List<String> inputList,long noofrows,Dataset<Row> nullvalues) {
		 
		 List<Column> list = new ArrayList<Column>();
		 for (String i:inputList)
			{
				list.add(lit(noofrows-Integer.parseInt(nullvalues.first().getAs(i))));
			}
		 
	     return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
	        //   functions.lit(noofrows-Integer.parseInt(nullvalues.first().getAs("Gender")))
	    }
	 
	 public static Dataset<Row> dataPreparing(Dataset<Row> inputdata) {
		 
		 inputdata = inputdata.na()
					.fill(inputdata.groupBy("Gender").count().orderBy(desc("Gender")).first().get(0).toString(),new String[] {"Gender"}).na()
					.fill(inputdata.groupBy("Married").count().orderBy(desc("Married")).first().get(0).toString(),new String[] {"Married"}).na()
					.fill(inputdata.groupBy("Dependents").count().orderBy(desc("Dependents")).first().get(0).toString(),new String[] {"Dependents"}).na()
					.fill(inputdata.groupBy("Self_Employed").count().orderBy(desc("Self_Employed")).first().get(0).toString(),new String[] {"Self_Employed"}).na()
					.fill((double)(int)inputdata.groupBy("Credit_History").count().orderBy(desc("Credit_History")).first().get(0),new String[] {"Credit_History"}).na()
					.fill(inputdata.stat().approxQuantile("LoanAmount",new double[] {0.5}, 0.000000000005)[0],new String[] {"LoanAmount"}).na()
					.fill(inputdata.stat().approxQuantile("Loan_Amount_Term",new double[] {0.5}, 0.000000000005)[0],new String[] {"Loan_Amount_Term"});
		 
//		 long noofrows = inputdata.count();
//			
//			
//			Dataset<Row> nullvalues = inputdata.describe();
//			
//			
//			nullvalues.filter(nullvalues.col("summary").equalTo("count"))
//				.selectExpr(convertListToSeq(Arrays.asList(inputdata.columns())))
//				.withColumns(convertListToSeq(Arrays.asList(inputdata.columns())), convertListToSe(Arrays.asList(inputdata.columns()),noofrows,nullvalues))
//				.show();
		 
		 inputdata = inputdata.drop("Loan_ID");
			StringIndexer indexer = new StringIndexer();
			indexer.setInputCols(new String[] {"Gender","Married","Dependents","Education","Self_Employed","Credit_History","Property_Area"})
				.setOutputCols(new String[] {"GenderIn","MarriedIn","DependentsIn","EducationIn","Self_EmployedIn","Credit_HistoryIn","Property_AreaIn"});
				
			inputdata = indexer.fit(inputdata).transform(inputdata);
			
			

			
			
			OneHotEncoder encoder = new OneHotEncoder();
			encoder.setInputCols(new String[] {"GenderIn","MarriedIn","DependentsIn","EducationIn","Self_EmployedIn","Credit_HistoryIn","Property_AreaIn"})
				.setOutputCols(new String[] {"GenderVec","MarriedVec","DependentsVec","EducationVec","Self_EmployedVec","Credit_HistoryVec","Property_AreaVec"});
			inputdata = encoder.fit(inputdata).transform(inputdata);
			
			inputdata = inputdata.withColumn("total_income", col("ApplicantIncome").$plus(col("CoapplicantIncome")))
			       .withColumn("emi", col("LoanAmount").$times(0.09).$times(pow(1.09,col("Loan_Amount_Term")).$div(pow(1.09,col("Loan_Amount_Term").$minus(1)))));
			
			
			VectorAssembler vectorAssembler = new VectorAssembler()
					.setInputCols(new String[] {"GenderVec","MarriedVec","DependentsVec","EducationVec","Self_EmployedVec","Credit_HistoryVec","Property_AreaVec","total_income","emi"})
					.setOutputCol("features");
			
			inputdata = vectorAssembler.transform(inputdata);
			
				
			
			
			return inputdata;
		}
	 
	 
	 
	 public static void insights(Dataset<Row> inputdata)
	 {
		 inputdata.printSchema();
		 inputdata.show(5);
			String[] columnnames = inputdata.columns();
			for(String i:columnnames)
				System.out.print(i+", ");
			System.out.println();
			
			//inputdata.groupBy("Loan_Status").count().show();
			inputdata.groupBy("Gender").count().show();
			inputdata.groupBy("Married").count().show();
			inputdata.groupBy("Dependents").count().show();
			inputdata.groupBy("Education").count().show();
			inputdata.groupBy("Self_Employed").count().show();
			inputdata.groupBy("Credit_History").count().show();
			inputdata.groupBy("Property_Area").count().show();
			
			
			long noofrows = inputdata.count();
			
		
			Dataset<Row> nullvalues = inputdata.describe();
			
			
			nullvalues.filter(nullvalues.col("summary").equalTo("count"))
				.selectExpr(convertListToSeq(Arrays.asList(inputdata.columns())))
				.withColumns(convertListToSeq(Arrays.asList(inputdata.columns())), convertListToSe(Arrays.asList(inputdata.columns()),noofrows,nullvalues))
				.show();

	 }

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder()
				.appName("Loan Prediction")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.master("local[*]").getOrCreate();
		
		Dataset<Row> trainandvalidationdata = spark.read()
				.option("header",true)
				.option("inferSchema", true)
				.csv("src/main/resources/input/loan_approval.csv");
		
		Dataset<Row> testData = spark.read()
				.option("header",true)
				.option("inferSchema", true)
				.csv("src/main/resources/input/loan_prediction.csv");
		
		
		//insights(trainandvalidationdata);
		//insights(testData);
		trainandvalidationdata = dataPreparing(trainandvalidationdata);
		testData = dataPreparing(testData);
		
		
		
		
		
		StringIndexer indexer = new StringIndexer();
		indexer.setInputCols(new String[] {"Loan_Status"})
			.setOutputCols(new String[] {"Loan_StatusIn"});
			
		trainandvalidationdata = indexer.fit(trainandvalidationdata).transform(trainandvalidationdata);

		trainandvalidationdata =  trainandvalidationdata
				.select("Loan_StatusIn","features")
				.withColumnRenamed("Loan_StatusIn", "label");
		
		
		testData = testData.select("features");
		
		trainandvalidationdata.show();
		testData.show();
		
		
		Dataset<Row>[] dataSplits = trainandvalidationdata.randomSplit(new double[] {0.8,0.2});
		Dataset<Row> modelInputData = dataSplits[0];
		Dataset<Row> holdOutData = dataSplits[1];
		
		
		LogisticRegression logisticRegression = new LogisticRegression();
		
		ParamGridBuilder paramGridBuilder = new ParamGridBuilder();
		
		ParamMap[] paramMap = paramGridBuilder.addGrid(logisticRegression.regParam(), new double[] {0.01,0.1,0.3,0.5,0.7,1})
				.addGrid(logisticRegression.elasticNetParam(),new double[] {0,0.5,1})
				.build();
		
		TrainValidationSplit tvs = new TrainValidationSplit();
		tvs.setEstimator(logisticRegression)
			.setEvaluator(new BinaryClassificationEvaluator())   //     new RegressionEvaluator().setMetricName("r2")
			.setEstimatorParamMaps(paramMap)
			.setTrainRatio(0.9);
		
		TrainValidationSplitModel model = tvs.fit(modelInputData);
		
		LogisticRegressionModel lrModel = (LogisticRegressionModel) model.bestModel();
		System.out.println("The accuracy value is "+ lrModel.summary().accuracy());
		
		System.out.println("coefficients : "+ lrModel.coefficients()+" intercept :"+lrModel.intercept());
		System.out.println("reg parma : "+lrModel.getRegParam()+" elastic net param : "+lrModel.getElasticNetParam());

		
		LogisticRegressionSummary summary = lrModel.evaluate(holdOutData);
		
//		double truePositives = summary.truePositiveRateByLabel()[1];  // summary.weightedFMeasure();
//		double falsePositives = summary.falsePositiveRateByLabel()[0];  //summary.weightedPrecision();
//		
//		
//		System.out.println(truePositives/(truePositives+falsePositives));    
//		System.out.println(falsePositives/(truePositives+falsePositives));
		System.out.println(summary.accuracy());
		
		Dataset<Row> testPrediction =  lrModel.transform(testData).select("prediction");
		testPrediction.show();	
		
		
		
		/*  Decision Tree  */
		
		DecisionTreeRegressor decisionTreeRegression = new DecisionTreeRegressor();
		
		paramMap = paramGridBuilder.addGrid(decisionTreeRegression.maxDepth(), new int[] {5,10,15,20})
				.addGrid(decisionTreeRegression.maxBins(),new int[] {2,5,10,15})
				.build();
		
		tvs = new TrainValidationSplit();
		tvs.setEstimator(decisionTreeRegression)
			.setEvaluator(new MulticlassClassificationEvaluator())
			.setEstimatorParamMaps(paramMap)
			.setTrainRatio(0.9);
		
		model = tvs.fit(modelInputData);
		
		DecisionTreeRegressionModel lrModel1 = (DecisionTreeRegressionModel) model.bestModel();
		
		lrModel1.transform(modelInputData).show();
		
		
		
	}
	
	
	
	

}
