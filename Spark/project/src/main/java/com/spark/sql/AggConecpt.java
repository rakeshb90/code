package com.spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

import java.util.List;

public class AggConecpt {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:/Users/Rakesh/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark=SparkSession.builder().appName("FirstSparkSQLApp")
				           .master("local[*]")
				           .config("key","value")
				           .getOrCreate();
		Dataset<Row> csvData=spark.read()
				.option("header",true)
//				.option("inferSchema",true)
				.csv("src/main/resources/inputdata/cars_sampled.csv");
		csvData.printSchema();
		csvData.show();
//		csvData.groupBy("abtest").max("price").show();// activaly work only with Integer or uncomment 
//		csvData.groupBy("abtest").mean("price").show();// the inferScema true line
		
		Dataset<Row>priceData=csvData
//				.groupBy("abtest")
				.agg(max(col("price").cast(DataTypes.IntegerType)).alias("Max_Price"),
				min(col("price").cast(DataTypes.IntegerType)).alias("Min_Price"),
				mean(col("price").cast(DataTypes.IntegerType)).alias("Mean_Value_Price"),
				avg(col("price").cast(DataTypes.IntegerType)).alias("Average_Price"),
				sum(col("price").cast(DataTypes.IntegerType)).as("Total_Price"));
		
		priceData.show();
		List<String> priceList=(csvData.agg(max(col("price").cast(DataTypes.IntegerType)).alias("Max_Price")).map(row->row.mkString(),Encoders.STRING()).collectAsList());
		int maxPrice=Integer.parseInt(priceList.get(0));
		priceList=(csvData.agg(min(col("price").cast(DataTypes.IntegerType))).map(row->row.mkString(),Encoders.STRING()).collectAsList());
		int minPrice=Integer.parseInt(priceList.get(0));
		int maxMinDiff=maxPrice-minPrice;
		System.out.println("max-Min value for Car price: "+maxMinDiff);
//		
		
		for(String row:priceList)
			System.out.println("Row value: "+row);
		
		
		
		
		
		
	   Dataset<Row> totalCost=csvData.agg(sum(col("price").cast(DataTypes.IntegerType)));
	   Dataset<Row> totalRow=csvData.agg(count(col("price").cast(DataTypes.IntegerType)));
	   Dataset<Row> avgOfCost=csvData.agg(avg(col("price").cast(DataTypes.IntegerType)));
//	   double x=(double) avgOfCost.collect();
	   List<String> strX=avgOfCost.map(row->row.mkString(), Encoders.STRING()).collectAsList();
	   System.out.println("Cost Average: "+strX);
	   avgOfCost.show();
	
	}

}
