package com.spark.basics;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class ClientMain {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf=new SparkConf().setAppName("StratWithSparkInJava")
				       .setMaster("local[*]");
		
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		JavaRDD rd1=sc.parallelize(Arrays.asList(12,10,11,40,20,30,122));
		System.out.println("Count of element: "+rd1.count());
		System.out.println("All element: "+rd1.collect());
		System.out.println("Sum of elements: "+rd1.reduce((a,b)->(int)a+(int)b));
		System.out.println("Square of elements: "+rd1.map(val->(int)val*(int)val).collect());
//		rd1.foreach(val->System.out.println(val));
		rd1.collect().forEach(System.out::println);
		System.out.println("Square of elements: "+rd1.map(val->new Tuple2(val,(int)val*(int)val)).collect());
       
		SparkSession spark=SparkSession.builder().config("key", "value").getOrCreate();
		Dataset<Row> csvData=spark.read()
				.option("header",true)
				.option("inferSchema",true)
				.csv("src/main/resources/inputdata/cars_sampled.csv");
		csvData.printSchema();
//		csvData.show();
		Dataset<Row> priceColumn=csvData.select("price");
		priceColumn.show();
	
		System.out.println("Total Price"+priceColumn.first());
		
		
		
		
		
		
		
	}

}
