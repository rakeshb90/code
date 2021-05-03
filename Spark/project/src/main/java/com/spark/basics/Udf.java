package com.spark.basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class Udf {
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark=SparkSession.builder()
				.appName("Loan Approval Prediction")
				.master("local[*]")
				.config("spark.some.config.option","some-value")
				.getOrCreate();
		Dataset<Row> approvalData=spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/input/loan_approval.csv");
		Dataset<Row> predictionData=spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/input/loan_prediction.csv");
		approvalData.printSchema();
//		for(String col:csvData.columns()) {
//		approvalData.filter("Credit_History is null").show();
//			if(col.isEmpty())
//				System.out.println("Column "+col);
//		}
		
//		approvalData=approvalData
//				.na().fill((approvalData.groupBy("Gender").count()
//						.orderBy("Gender").first().get(0).toString()));
//		approvalData.show();
		
		//UDF
		spark.udf().register("nullIdColumn",new UDF1<Long,Object>(){
			@Override
			public Boolean call(Long id) {
				return id<5;
			}
		},DataTypes.BooleanType);
		
//		approvalData.createOrReplaceTempView("nullIdTable");
//		spark.range(1, 10).createOrReplaceTempView("test");
//		spark.sql("SELECT *FROM test WHERE nullIdColumn(id)").show();
		
		
		spark.close();

	}

}
