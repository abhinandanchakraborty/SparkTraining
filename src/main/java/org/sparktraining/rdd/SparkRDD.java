package org.sparktraining.rdd;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkRDD implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	private static SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("wordCount");
	private static JavaSparkContext sc = new JavaSparkContext(conf);


	public void hdfsWordCountSave(String hdfsInputPath, String hdfsOutputPath) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data.
		JavaRDD<String> input = sc.textFile(hdfsInputPath);
		// Split up into words.
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});

		// Transform into word and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String x) {
				return new Tuple2(x, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});

		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(hdfsOutputPath);
	}
	
	
	
	public void hdfsWordCountPrint(String hdfsInputPath) {

		// Load our input data.
		JavaRDD<String> input = sc.textFile(hdfsInputPath);
		// Split up into words.
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});

		// Transform into word and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String x) {
				return new Tuple2(x, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
		
			private static final long serialVersionUID = 1L;

			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});
		
		
        // Print all Items
		List<Tuple2<String,Integer>> collectWordCount = counts.collect();
		for (Tuple2<String,Integer> tuple : collectWordCount) {
			System.out.println(String.format("Word [%s] count [%d].", tuple._1(), tuple._2()));
		}
	}
	
	public static void main(String args[]){
		SparkRDD rdd = new SparkRDD();
		rdd.hdfsWordCountPrint("/home/abhinandan/Desktop/source.csv");
	}
}
