package com.rakesh.spark.wordCount;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCounter {
	public static void  wordCount(String inputFileName) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf sparkConf=new SparkConf().setMaster("local").setAppName("Word Counter App");
		JavaSparkContext context=new JavaSparkContext(sparkConf);
		JavaRDD<String>inputFile=context.textFile(inputFileName);
		JavaRDD<String>words=inputFile.flatMap(content->Arrays.asList(content.split(" ")));
		JavaPairRDD countWord=words.mapToPair(x->new Tuple2(x,1)).reduceByKey((a,b)->(int)a+(int)b);
		countWord.foreach(x->System.out.println(x));
		countWord.saveAsTextFile("outputFile");
//		countWord.foreach(x->System.out.println(x));
	}
	 public static void main( String[] args )
	    {
	        System.out.println( "***********Word count spark project********" );
	        String inputFileName="input.txt";
	        if(inputFileName.length()==0) {
	        	System.out.println("input File required.... ");
	        	System.exit(0);
	        }
	       wordCount(inputFileName);
	       
	    }

}
