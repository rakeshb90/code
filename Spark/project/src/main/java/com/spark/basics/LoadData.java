package com.spark.basics;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class LoadData {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf=new SparkConf().setAppName("LoadDataFromDisk")
				       .setMaster("local[*]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		JavaRDD<String> inputRDD=sc.textFile("src/main/resources/inputdata/input.txt");
		inputRDD.collect().forEach(System.out::println);
		JavaRDD<String> weatehrRDD=sc.textFile("src/main/resources/inputdata/weather.txt");
		JavaRDD<String> sentRDD=weatehrRDD.map(line->line.replaceAll("[^a-zA-Z\\s]", ""));
		sentRDD.take(50).forEach(System.out::println);
		sentRDD.flatMap(line->Arrays.asList(line.split(" ")).iterator()).filter(word->word.length()>2).collect().forEach(System.out::println);
//        JavaRDD<String> sortedRDD=sentRDD.flatMap(line->Arrays.asList(line.split(" ")).iterator()).filter(word->word.length()>2).map(word->new Tuple2(word,1));
        System.out.println("*******************Sorted RDD****************");
//        sortedRDD.collect().forEach(System.out::println);
	}

}
