package org.spark.java.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.base.Optional;

import scala.Tuple2;

public class SparkJoins {

	@SuppressWarnings({ "serial", "resource" })
	public static void main(String[] args) {
		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"));
		
        JavaRDD<String> customerInputFile = sc.textFile("E:/Software/Spark/first-example/src/test/resources/customers_data.txt");
        JavaPairRDD<String, String> customerPairs = customerInputFile.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String[] customerSplit = s.split(",");
                return new Tuple2<String, String>(customerSplit[0], customerSplit[1]);
            }
        }).distinct();

        JavaRDD<String> transactionInputFile = sc.textFile("E:/Software/Spark/first-example/src/test/resources/transactions_data.txt");
        JavaPairRDD<String, String> transactionPairs = transactionInputFile.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String[] transactionSplit = s.split(",");
                return new Tuple2<String, String>(transactionSplit[2], transactionSplit[3]+","+transactionSplit[1]);
            }
        });

        //Default Join operation (Inner join)
        JavaPairRDD<String, Tuple2<String, String>> joinsOutput = customerPairs.join(transactionPairs);

        //Left Outer join operation
       JavaPairRDD<String, Tuple2<String, Optional<String>>> leftJoin = customerPairs.leftOuterJoin(transactionPairs);
       
       JavaPairRDD<String, String> leftjoinRDD = leftJoin
                 .mapToPair(tuple -> {
                     if (tuple._2()._2().isPresent()) {
                         //do your operation and return
                         return new Tuple2<String, String>(tuple._1(), tuple._2()._1());
                     } else {
                         return new Tuple2<String, String>(tuple._1(), "not present");
                     }
                 });
    
  
     //Right Outer join operation
     JavaPairRDD<String, Tuple2<Optional<String>, String>> rightjoinRDD = customerPairs.rightOuterJoin(transactionPairs);
     
     JavaPairRDD<String, String> rightJoinOutput = rightjoinRDD
             .mapToPair(tuple -> {
                 if (tuple._2()._1().isPresent()) {
                     //do your operation and return
                     return new Tuple2<String, String>(tuple._1(), tuple._2()._2());
                 } else {
                     return new Tuple2<String, String>(tuple._1(), "not present");
                 }
             });


     
     System.out.println("Joins function Output: "+joinsOutput.collect());
     System.out.println("Left OuterJoin function Output: "+leftjoinRDD.collect());
     System.out.println("Right OuterJoin function Output: "+rightJoinOutput.collect());

     sc.close();

	}

}
