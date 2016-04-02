package org.spark.java.examples;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class JavaSparkSQL {
	public static class Person implements Serializable {
		private String name;
		private int age;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}
	}

	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(ctx);

		System.out.println("=== Data source: RDD ===");
		// Load a text file and convert each line to a Java Bean.
		
		JavaRDD<Person> people = ctx.textFile("E:/Software/Spark/first-example/src/test/resources/people.txt")
				.map(new Function<String, Person>() {

					public Person call(String line) {
						String[] parts = line.split(",");

						Person person = new Person();
						person.setName(parts[0]);
						person.setAge(Integer.parseInt(parts[1].trim()));

						return person;
					}
				});

		// Apply a schema to an RDD of Java Beans and register it as a table.
		DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
		schemaPeople.registerTempTable("people");

		// SQL can be run over RDDs that have been registered as tables.
		DataFrame df = sqlContext.sql("SELECT name,age FROM people WHERE age >= 13 AND age <= 19");
/*
		// Show the content of the DataFrame
		df.show();
		
		// Select everybody, but increment the age by 1
		df.select(df.col("name"), df.col("age").plus(1)).show();
		
		// Select people older than 21
		df.filter(df.col("age").gt(21)).show();

		// Count people by age
		df.groupBy("age").count().show();
*/		
		
		// The results of SQL queries are DataFrames and support all the normal
		// RDD operations.
		// The columns of a row in the result can be accessed by ordinal.
		List<String> teenagerNames = df.toJavaRDD().map(new Function<Row, String>() {

			public String call(Row row) {
				return "Name: " + row.getString(0) + " --> Age:" +  row.getInt(1);
			}
		}).collect();
		for (String name : teenagerNames) {
			System.out.println(name);
		}

  /* 	System.out.println("=== Data source: Parquet File ===");
		// DataFrames can be saved as parquet files, maintaining the schema
		// information.
		schemaPeople.write().parquet("people.parquet");

		// Read in the parquet file created above.
		// Parquet files are self-describing so the schema is preserved.
		// The result of loading a parquet file is also a DataFrame.
		DataFrame parquetFile = sqlContext.read().parquet("people.parquet");

		// Parquet files can also be registered as tables and then used in SQL
		// statements.
		parquetFile.registerTempTable("parquetFile");
		DataFrame teenagers2 = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19");
		teenagerNames = teenagers2.toJavaRDD().map(new Function<Row, String>() {

			public String call(Row row) {
				return "Name: " + row.getString(0);
			}
		}).collect();
		for (String name : teenagerNames) {
			System.out.println(name);
		}
  */
		ctx.stop();
	}
}
