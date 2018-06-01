package com.kv.j8;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MaxTempSparkJava8 {

	private static JavaSparkContext sc;

	public static void main(String[] argf) {
		
		String args[] = {"/Users/karan.verma/Documents/backups/h/hContent/input/ncdc/micro-tab/sample.txt",
		"/Users/karan.verma/Documents/backups/h/hadoop/output_MaxTemperatureSpark6"}; 

		if (args.length != 2) {
			System.err.println("Usage: MaxTemperatureSpark <input path> <output path>");
			System.exit(-1);
		}
		
		SparkConf conf = new SparkConf();
		sc = new JavaSparkContext("local", "MaxTempSparkJava8", conf);
		
		JavaRDD<String> lines = sc.textFile(args[0]);
		
		JavaRDD<String[]> records = lines.map(l -> l.split("\t"));
		records.collect().stream().forEach(r -> System.out.println("record  "+r[0]));
		//System.out.println("records = "+ );
		JavaRDD<String[]> filtered = records.filter(f -> f.length > 1 && f[0] != "9999" && f[1].matches("[01459]"));
		
		JavaPairRDD<Integer, Integer> tuples = filtered.mapToPair(
														f -> new Tuple2<Integer, Integer>(
																Integer.parseInt(f[0]), 
																Integer.parseInt(f[1])
																));
		
		JavaPairRDD<Integer, Integer> maxTemps = tuples.reduceByKey((t1, t2) -> Math.max(t1, t2));
		List<Tuple2<Integer, Integer>> temps =  maxTemps.collect();
		
		System.out.println("temps = "+temps.size());
		
		temps.forEach( t -> System.out.println(t._1+" "+t._2));
		
	}

}
