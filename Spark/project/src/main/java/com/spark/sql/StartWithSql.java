package com.spark.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StartWithSql {

	public static void main(String[] args) {
//		System.setProperty("hadoop.home.dir", "C:/Users/Rakesh/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark=SparkSession.builder().appName("FirstSparkSQLApp")
				           .master("local[*]")
				           .config("spark.sql.warehouse.dir","C:/Temp/")
				           .getOrCreate();
		
		// data load from file
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/inputdata/cars_sampled.csv");
		
	
//		dataset.show();
		dataset.printSchema();
		Row firstRow=dataset.first();
//		System.out.println("first row Car name: "+firstRow.get(1));
		System.out.println("first row Car name: "+firstRow.getAs("name"));
//		dataset.filter("abtest='control' and fuelType='diesel' and brand='bmw'").show();
		System.out.println("Number of rows: "+dataset.count());
		dataset.filter(row->row.getAs("brand").equals("bmw")).show();
//		Column carNames=col("name");
//	    dataset.filter(col("name").equalTo("bmw").and(col("fuelType").equalTo("diesel"))).show();
        // ful sql syntex
		dataset.createOrReplaceTempView("my_cars_view");
//		spark.sql("select name as Car_Name,price,model,brand from my_cars_view where abtest='control'").show();
		spark.sql("select max(price)as Max_price,avg(kilometer)as avg_distance from my_cars_view").show();
		spark.sql("select distinct(yearOfRegistration) from my_cars_view order by yearOfRegistration desc").show();
		spark.close();
	}

}
