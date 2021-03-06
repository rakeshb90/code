package com.spark.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;





public class PivotBasics {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark=SparkSession.builder().appName("Pivot in Spark")
				.master("local[*]").config("somthing", "something").getOrCreate();
		Dataset<Row> csvData=spark.read().option("header", true)
				.csv("src/main/resources/inputdata/cars_sampled.csv");

		
		csvData=csvData.groupBy("brand","fuelType")
		.count().as("total").orderBy("brand");
		
		
		csvData.show(100);
		
		
		
		
		
		
		
		
		
		
		
		
		
//		csvData=csvData.select("abtest","brand");
		
//		Dataset<Row> pivot=csvData.groupBy("abtest").pivot("monthOfRegistration").count();
//		spark.udf().register("effective",(String fuel)->fuel.equals("diesel"),DataTypes.BooleanType);
//		csvData.withColumn("Effective_Month",callUDF("effective",csvData.col("fuelType")) );
//		csvData.agg(effective(csvData.col("fuelType")));
//		pivot.show();
		spark.close();
	}

}
