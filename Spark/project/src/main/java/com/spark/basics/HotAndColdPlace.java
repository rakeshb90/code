package com.spark.basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class HotAndColdPlace {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf=new SparkConf().setAppName("Hot and Cold Area")
				.setMaster("local[*]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		JavaRDD<String> weatherInput=sc.textFile("src/main/resources/inputdata/weather-stations.csv");
//		weatherInput=weatherInput.map(x->x.replace("", replacement))
		weatherInput.take(20).forEach(System.out::println);

	}

}
