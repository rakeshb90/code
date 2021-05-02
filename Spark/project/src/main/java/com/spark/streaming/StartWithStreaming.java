package com.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class StartWithStreaming {

	public static void main(String[] args) throws InterruptedException {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf=new SparkConf().setAppName("start with streaming")
				.setMaster("local[*]");
		JavaStreamingContext sc=new JavaStreamingContext(conf,Durations.seconds(10));
		JavaReceiverInputDStream<String> data = sc.socketTextStream("localhost", 8989);
		JavaDStream<String> result = data.map(line->line);
//		System.out.println("Count of Log messages: "+result.count());
		JavaPairDStream<String,Long> mapReduce=result.mapToPair(logMessage->new Tuple2(logMessage.split(",")[0],1L));
//		mapReduce=mapReduce.reduceByKey((a,b)->a+b);// works for last batch
		mapReduce=mapReduce.reduceByKeyAndWindow((a,b)->a+b,Durations.minutes(1));// works for jobs last one minute
		mapReduce.print();
		sc.start();
		sc.awaitTermination();

	}

}
