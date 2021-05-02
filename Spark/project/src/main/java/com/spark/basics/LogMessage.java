package com.spark.basics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import scala.Tuple2;

public class LogMessage {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf=new SparkConf().setAppName("LogMessage")
				       .setMaster("local[*]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		List<String> logMessage=new ArrayList<>();
		logMessage.add("WARN: Tuesday 4 september 0405");
		logMessage.add("WARN: Tuesday 4 september 0406");
		logMessage.add("WARN: Tuesday 4 september 0408");
		logMessage.add("FETAL: Tuesday 4 september 0405");
		logMessage.add("INFO: Tuesday 4 september 0407");
		logMessage.add("FETAL: Tuesday 7 september 0409");
		JavaRDD<String>logRdd=sc.parallelize(logMessage);
		JavaPairRDD<String, Integer> pairRdd=logRdd
				.mapToPair(x->new Tuple2<>(x.split(":")[0],1))
				.reduceByKey((a,b)->(Integer)a+(Integer)b);
		pairRdd.collect().forEach(System.out::println);
//				//using group by key
//				.groupByKey().foreach(tuple->tuple._1+" has "+Iterables.size(tuple._2)).collect();
		JavaRDD<String>logFlatMapRdd=sc.parallelize(logMessage)
				.flatMap(line->Arrays.asList(line.split(" ")).iterator());
		JavaRDD<String> filterRDD=logFlatMapRdd.filter(word->word.length()>1);
		filterRDD.collect().forEach(System.out::println);
	}

}
