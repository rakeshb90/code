package com.project.loanApprovalPrediction;

import static org.apache.spark.sql.functions.*;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class ModelFieldSelection {
	
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
	public static void countNull(Dataset<Row>csvData) {
		for(String col:csvData.columns()) {
			System.out.println("Count of null in Column "+col+" is: "+csvData.select(col).where(col(col).isNull()).count());
		}
	}
	public static Dataset cleanData(Dataset loanData) {
		loanData=loanData.na()
				.fill(loanData.groupBy("Gender").count().orderBy(desc("Gender")).first().get(0).toString(),new String[] {"Gender"}).na()
				.fill(loanData.groupBy("Married").count().orderBy(desc("Married")).first().get(0).toString(),new String[] {"Married"}).na()
				.fill(loanData.groupBy("Dependents").count().orderBy(desc("Dependents")).first().get(0).toString(),new String[] {"Dependents"}).na()
				.fill(loanData.groupBy("Self_Employed").count().orderBy(desc("Self_Employed")).first().get(0).toString(),new String[] {"Self_Employed"}).na()
				.fill((double)(int)loanData.groupBy("Credit_History").count().orderBy(desc("Credit_History")).first().get(0),new String[] {"Credit_History"}).na()
				.fill(loanData.stat().approxQuantile("LoanAmount",new double[] {0.5}, 0.000000000005)[0],new String[] {"LoanAmount"}).na()
				.fill(loanData.stat().approxQuantile("Loan_Amount_Term",new double[] {0.5}, 0.000000000005)[0],new String[] {"Loan_Amount_Term"});
			
			return loanData;
			
	}
		public static Dataset stringIndexing(Dataset csvData) {

		 csvData=csvData.drop("Loan_ID");
			StringIndexer dataIndex=new StringIndexer()
					.setInputCols(new String[] {"Loan_Status","Gender","Self_Employed","Married","Education","Property_Area","Dependents"})
					.setOutputCols(new String[] {"Loan_StatusIndex","GenderIndex","Self_EmployedIndex","MarriedIndex","EducationIndex","Property_AreaIndex","DependentsIndex"})
					.setHandleInvalid("keep");
			csvData=dataIndex.fit(csvData).transform(csvData);
         csvData=csvData.drop(new String[] {"Loan_Status","Gender","Self_Employed","Married","Education","Property_Area","Dependents"});
     return csvData;
	}
		public static Dataset<Row> indexToVector(Dataset<Row> csvData) {
			
			OneHotEncoder encoder=new OneHotEncoder()
					.setInputCols(new String[] {"GenderIndex","Self_EmployedIndex","MarriedIndex","EducationIndex","Property_AreaIndex","DependentsIndex"})
					.setOutputCols(new String[] {"GenderVector","Self_EmployedVector","MarriedVector","EducationVector","Property_AreaVector","DependentsVector"});
			csvData=encoder.fit(csvData).transform(csvData);
			return csvData;

		}
public static Dataset<Row> getModelInput(Dataset<Row> csvData){
	VectorAssembler assembler=new VectorAssembler()
			.setInputCols(new String[] {"GenderVector","MarriedVector","EducationVector","Self_EmployedVector","ApplicantIncome","CoapplicantIncome","Loan_Amount_Term","Credit_History","Property_AreaVector","LoanAmount"})
			.setOutputCol("features");
	
	Dataset<Row> modelInput=assembler.transform(csvData)
			.select("Loan_StatusIndex","features")
			.withColumnRenamed("Loan_StatusIndex", "label");
    return modelInput;
}
public static Dataset<Row> getModelInputForLinearReg(Dataset<Row> csvData){
	VectorAssembler assembler=new VectorAssembler()
			.setInputCols(new String[] {"ApplicantIncome","CoapplicantIncome","EducationVector","DependentsVector","Self_EmployedVector","MarriedVector"})
			.setOutputCol("features");
	
	Dataset<Row> modelInput=assembler.transform(csvData)
			.select("LoanAmount","features")
			.withColumnRenamed("LoanAmount", "label");
    return modelInput;
}
	
	public static void checkCorrelation(Dataset<Row> inputData) {
		 for(String col1:inputData.columns()) {
//			 
                 String col2="LoanAmount";
				 System.out.println("Correlation in between "+col1+" and "+col2+" : "+Math.abs(inputData.stat().corr(col1, col2)));
//			 
		 }
		
	}


	public static void main(String[] args) {
		Dataset<Row> csvData=init();
		
		csvData.printSchema();
		csvData.show();
		
		countNull(csvData);
		csvData=cleanData(csvData);
		System.out.println("**************After Cleaning**************");
		csvData.printSchema();
		csvData.show(); 
		System.out.println("**************After Indexing**************");
		csvData=stringIndexing(csvData);
		csvData.printSchema();
		csvData.show();
		System.out.println("**************Correlation Result**************");
		checkCorrelation(csvData);
		System.out.println("**************After changing index to vector**************");
		csvData=indexToVector(csvData);
		csvData.printSchema();
		csvData.show();
		System.out.println("**************Model Input**************");
		csvData=getModelInput(csvData);
		csvData.printSchema();
		csvData.show();
		
		}

}
