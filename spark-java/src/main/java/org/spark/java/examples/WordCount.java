package org.spark.java.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

public class WordCount implements Serializable {
	private static final FlatMapFunction<String, String> WORDS_EXTRACTOR = new FlatMapFunction<String, String>() {
		public Iterable<String> call(String s) throws Exception {
			return Arrays.asList(s.split(" "));
		}
	};

	private static final PairFunction<String, String, Integer> WORDS_MAPPER = new PairFunction<String, String, Integer>() {
		public Tuple2<String, Integer> call(String s) throws Exception {
			return new Tuple2<String, Integer>(s, 1);
		}
	};

	private static final Function2<Integer, Integer, Integer> WORDS_REDUCER = new Function2<Integer, Integer, Integer>() {
		public Integer call(Integer a, Integer b) throws Exception {
			return a + b;
		}
	};


	private static class TupleComparator implements Comparator<Tuple2<Integer, String>>, Serializable {
		public int compare(Tuple2<Integer, String> tuple1, Tuple2<Integer, String> tuple2) {
			return tuple1._1 < tuple2._1 ? 0 : 1;
		}
	}
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Please provide the input & output file full path as argument");
			System.exit(0);
		}

		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> file = context.textFile(args[0]);
		JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR);
		JavaPairRDD<String, Integer> pairs = words.mapToPair(WORDS_MAPPER);
		JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);
		
		// to enable sorting by value (count) and not key -> value-to-key
				// conversion pattern
		/*		JavaPairRDD<Tuple2<Integer, String>, Integer> countInKey = counter
						.mapToPair(a -> new Tuple2(new Tuple2<Integer, String>(a._2, a._1), null)); 

				List<Tuple2<Tuple2<Integer, String>, Integer>> wordSortedByCount = countInKey
						.sortByKey(new TupleComparator(), false).collect();

				List<String> result = new ArrayList<>();
				IntStream.range(0, 5).forEach(i -> result.add(wordSortedByCount.get(i)._1._2));
		
		System.out.println("Word Count is successful" +result);
		*/
		counter.saveAsTextFile(args[1]);
		

	}

	
}
