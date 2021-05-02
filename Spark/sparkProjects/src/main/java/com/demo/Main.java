package com.demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf=new SparkConf().setAppName("Demo App")
				.setMaster("local[*]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		JavaRDD<String> input=sc.textFile("src/main/resources/input/weather.txt");
		input.take(20).forEach(System.out::println);
		

	}

}
