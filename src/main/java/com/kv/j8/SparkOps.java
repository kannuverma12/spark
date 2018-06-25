package com.kv.j8;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkOps {
	private static JavaSparkContext sc;

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		sc = new JavaSparkContext("local", "SparkOps", conf);

		
		//Squaring the values in RDD
		System.out.println("******Squaring the values in RDD****");
		
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4));
		
		JavaRDD<Integer> result = rdd.map(x -> x*x);
		
		System.out.println(StringUtils.join(result.collect(), ","));
		
		System.out.println();
		//Flat Map
		System.out.println("******Flat Map in RDD****");
		
		JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hi")); 
		
		
		//JavaRDD<String> words = lines.flatMap(l -> Arrays.asList(l.split("")));
		JavaRDD<String> words = lines.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
		
		JavaPairRDD<String, Integer> result1 = words.mapToPair(x -> new Tuple2<String, Integer>(x,1))
													.reduceByKey((x,y) -> x+y);
		System.out.println(StringUtils.join(result1.collect(), ","));
		
		
		System.out.println();
		System.out.println("******Combine By Key in RDD****");
		
		/*
		 * The API takes three functions (as lambda expressions in Python or anonymous functions in Scala), namely,

			Create combiner function: x
			Merge value function: y
			Merge combiners function: z
			and the API format is combineByKey(x, y, z).
		 */
		
		
		
		System.out.println();
		System.out.println("*****Custom parallelism****");
		
	}
	
	

}
