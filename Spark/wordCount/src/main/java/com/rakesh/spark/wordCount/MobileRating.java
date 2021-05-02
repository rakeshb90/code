package com.rakesh.spark.wordCount;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

public class MobileRating {
	public static void mobileRatter(String fileName){
		 System.out.println( "***********Mobile Rating spark project********" );
		SparkConf sparkConf=new SparkConf().setMaster("local").setAppName("Mobile Ratting App");
		JavaSparkContext sc= new JavaSparkContext(sparkConf);
		JavaRDD<String> inputFile=sc.textFile(fileName);
		JavaRDD<List<String>> company=inputFile.map(line->Arrays.asList(line.split(" ")));
		JavaPairRDD countMobile=company.mapToPair(x->new Tuple2(Arrays.asList(x.get(1),x.get(2)),1)).reduceByKey((a,b)->(int)a+(int)b);
		countMobile.foreach(x->System.out.println(x));
		
		countMobile.saveAsTextFile("mobileOutputFile");
//		countMobile.foreach(x->System.out.println(x));
		
	}
	
	public static void main(String args[]) {
		String fN="mobileInput.txt";
		if(fN.length()<1) {
			System.out.println("Input file not valid");
			System.exit(0);
		}
		mobileRatter(fN);
	}
}
