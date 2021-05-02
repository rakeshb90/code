package com.spark.basics;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;
import scala.Tuple3;

public class JoinRDD {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf=new SparkConf().setAppName("LoadDataFromDisk")
				       .setMaster("local[*]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		List<Tuple2<Integer,String>>student=new ArrayList<>();
		student.add(new Tuple2(101,"Ram"));
		student.add(new Tuple2(102,"Ramesh"));
		student.add(new Tuple2(100,"Ramu"));
		List<Tuple2<Integer,Integer>>testMarks=new ArrayList<>();
		testMarks.add(new Tuple2(101,98));
		testMarks.add(new Tuple2(105,88));
		testMarks.add(new Tuple2(100,90));
		JavaPairRDD<Integer,String> studentRdd=sc.parallelizePairs(student);
		JavaPairRDD<Integer, Integer> marksRdd=sc.parallelizePairs(testMarks);
		JavaPairRDD<Integer,Tuple2<String,Integer>> innerJoinRdd=studentRdd.join(marksRdd);
		innerJoinRdd.collect().forEach(System.out::println);
//		JavaPairRDD<Integer,Tuple2<String,Optional<Integer>>>leftOuterJoinRdd=studentRdd.leftOuterJoin(testMarks);
//		JavaPairRDD<Integer,Tuple2<Optional<Integer>,String>>rightOuterJoinRdd=studentRdd.rightOuterJoin(testMarks);
//	    rightOuterJoinRdd.collect().forEach(System.out::println);
		JavaPairRDD<Tuple2<Integer, String>, Tuple2<Integer, Integer>>fullJoinRdd=studentRdd.cartesian(marksRdd);
		fullJoinRdd.collect().forEach(System.out::println);
		Scanner scanner=new Scanner(System.in);
		scanner.nextLine();
		sc.close();
	}

}
