package com.spark.basics;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class WordCounter {
	public static void  wordCount(String inputFileName) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark=SparkSession.builder().appName("House price prediction")
				.master("local[*]").config("spark.sql.warehouse.dir", "file:///C:/Temp")
				.getOrCreate();
		Dataset<Row> input=spark.read().option("header", true).csv("src/main/resources/inputdata/weather-stations.csv");
        input.show();
//        VectorAssembler va=new VectorAssembler();
        System.out.println("Number of rows "+input.count());
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
