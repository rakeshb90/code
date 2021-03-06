package com.rakesh.spark.wordCount;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PopularCourse {
	public static void popularCourse() {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf=new SparkConf().setAppName("Popular course determination app")
				                      .setMaster("local[*]");
	
		JavaSparkContext sc=new JavaSparkContext(conf);
		System.out.println( "***********Debug code********" );
		List<Tuple2<Long,Integer>>viewData=new ArrayList<>();
		List<Tuple2<Integer,Long>>chapterData=new ArrayList<>();
		
		viewData.add(new Tuple2<Long,Integer>(14L,96));
		viewData.add(new Tuple2<Long,Integer>(14L,97));
		viewData.add(new Tuple2<Long,Integer>(13L,96));
		viewData.add(new Tuple2<Long,Integer>(13L,96));
		viewData.add(new Tuple2<Long,Integer>(13L,96));
		viewData.add(new Tuple2<Long,Integer>(14L,99));
		viewData.add(new Tuple2<Long,Integer>(13L,100));
		
		chapterData.add(new Tuple2<Integer,Long>(96,1L));
		chapterData.add(new Tuple2<Integer,Long>(97,1L));
		chapterData.add(new Tuple2<Integer,Long>(98,1L));
		chapterData.add(new Tuple2<Integer,Long>(99,2L));
		for(int i=1;i<=10;i++)
		chapterData.add(new Tuple2<Integer,Long>(99+i,3L));
		
		
		JavaPairRDD<Long, Integer> viewRdd=sc.parallelizePairs(viewData);
		JavaPairRDD<Integer, Long> chapterRdd=sc.parallelizePairs(chapterData);
		
		//make distict user
		viewRdd=viewRdd.distinct();
		
		
		// make joinable viewRdd
		JavaPairRDD<Integer,Long> joinableViewRdd=viewRdd
				.mapToPair(x->new Tuple2<Integer,Long>(x._2,x._1));
		
		// join two rdd as <chapterId,<userId,courseId>>
		JavaPairRDD<Integer,Tuple2<Long,Long>> joinRdd=joinableViewRdd.join(chapterRdd);
		
		
		// make a pair rdd to get popularity<courseId,UserId>
		JavaPairRDD<Long,Long> popularityRdd=joinRdd
				.mapToPair(x->new Tuple2<Long,Long>(x._2._2,x._2._1));
		 
		// Result course with number of views
		JavaRDD<Long> couserIdRdd=popularityRdd.map(x->x._1);
		
		JavaPairRDD result=couserIdRdd.mapToPair(id->new Tuple2(id,1))
				.reduceByKey((a,b)->(int)a+(int)b);
		
		
		result.collect().forEach(System.out::println);
		
		sc.close();
	}

	public static void main(String[] args) {
    System.out.println( "***********spark project********" );
	  popularCourse();
	}

}
