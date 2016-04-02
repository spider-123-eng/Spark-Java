package org.spark.java.examples;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Java8WC {
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Please provide the input & output file full path as argument");
			System.exit(0);
		}

		SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> lines = context.textFile(args[0]);
		
		JavaRDD<String> search = context.textFile(args[0]).filter(s -> s.contains("revanth"));
		long count = search.count();
		System.out.println("Word occurence  " + count);
		
		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")));
		JavaPairRDD<String, Integer> counts = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
				.reduceByKey((x, y) -> x + y);

		counts.saveAsTextFile(args[1]);
	}
}
