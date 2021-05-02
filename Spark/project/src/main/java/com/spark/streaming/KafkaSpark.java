package com.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class KafkaSpark {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf=new SparkConf().setAppName("Kafka Integeration with Spark")
				.setMaster("local[*]");
		JavaStreamingContext sc=new JavaStreamingContext(conf,Durations.seconds(1));
//		KafkaUtils.createDstream();

	}

}
