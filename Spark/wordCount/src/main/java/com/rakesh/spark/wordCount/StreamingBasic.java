package com.rakesh.spark.wordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class StreamingBasic {
	public static void station(String inputFile) {
	SparkConf sparkConf=new SparkConf().setAppName("Weather analytical App")
			.setMaster("local[4]");
	JavaSparkContext sc=new JavaSparkContext(sparkConf);
	JavaRDD <String> input=sc.textFile(inputFile);
	JavaRDD<List<String>>lines=input.map(line->Arrays.asList(line.split(",")));
	List<Tuple1> stationInfo=lines.map(x->new Tuple1(Arrays.asList(x.get(0),x.get(1),x.get(2),x.get(22)))).collect();
	System.out.println("[Station_Name,Latitute,Longitute,Station_Number");
	stationInfo.forEach(x->System.out.println(x));
	((AbstractJavaRDDLike<String, JavaRDD<String>>) stationInfo).saveAsTextFile("stationOutput");
	}
	public static void main(String[] args) {
		String inputFile="weather.txt";
		if(inputFile.length()<1) {
			System.out.println("Input file required");
			System.exit(0);
		}
		station(inputFile);
	}
	
	
}
