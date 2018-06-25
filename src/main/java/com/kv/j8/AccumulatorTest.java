package com.kv.j8;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.assertj.core.util.Arrays;

public class AccumulatorTest {

	public static void main(String[] arg2s) {
		String args[] = {"/Users/karan.verma/Documents/backups/h/hContent/input/ncdc/micro-tab/sample.txt",
		"/Users/karan.verma/Documents/backups/h/hadoop/output_MaxTemperatureSpark6"}; 

		if (args.length != 2) {
			System.err.println("Usage: MaxTemperatureSpark <input path> <output path>");
			System.exit(-1);
		}
		
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext("local", "MaxTempSparkJava8", conf);
		
		JavaRDD<String> lines = sc.textFile(args[0]);

		
		System.out.println("******AccumulatorTest ****");
		
		final Accumulator<Integer> blanklines = sc.accumulator(0);
		JavaRDD<Object> callSigns = lines.flatMap(f -> {
															if(f.equals(""))
																blanklines.add(1);
															return Arrays.asList(f.toString().split(" ")).iterator();
															
		});
		
		callSigns.saveAsTextFile("output.txt");
		System.out.println("Blank lines: "+ blanklines.value());

	}

}
