package org.sparktraining.streaming;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.sparktraining.sparkconfig.SparkConfigInit;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.ConfigurationBuilder;

public class SparkStreamingTwitter implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static SparkConfigInit sparkConfig = new SparkConfigInit();
	private static SparkConf sparkConf = new SparkConf().setAppName("Spark Sesssion").setMaster("local[2]")
			.set("spark.serializer", KryoSerializer.class.getName());

	public void startTwitterStreaming(String consumerKey, String consumerSecret, String accessToken,
			String accessTokenSecret, String[] filters) throws InterruptedException {

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setOAuthConsumerKey(consumerKey); // Change the Key value, Token etc.
												// with your actual token value
		cb.setOAuthConsumerSecret(consumerSecret);
		cb.setOAuthAccessToken(accessToken);
		cb.setOAuthAccessTokenSecret(accessTokenSecret);

		Authorization auth = AuthorizationFactory.getInstance(cb.build());

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, auth, filters);

		JavaDStream<String> words = stream.flatMap(new FlatMapFunction<Status, String>() {
			@Override
			public Iterator<String> call(Status s) {
				return Arrays.asList(s.getText().split(" ")).iterator();
			}
		});

		JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String word) throws Exception {
				return word.startsWith("#");
			}
		});

		JavaPairDStream<String, Integer> hashTagCount = hashTags.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				// leave out the # character
				return new Tuple2<String, Integer>(s.substring(1), 1);
			}
		});

		JavaPairDStream<String, Integer> hashTagTotals = hashTagCount
				.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				}, new Duration(10000));
		
		
		

		hashTagTotals.print();

		jssc.start();
		jssc.awaitTermination();

	}

	public static void main(String args[]) throws InterruptedException {
		SparkStreamingTwitter tweetsStream = new SparkStreamingTwitter();
		String consumerKey = "4BqpCwjjQIfVjZI0YTefqjcze";
		String consumerSecret = "6u8vrNrKLmdH45U58wTAVP2IDCkYgMPBvj3MeZVHoHgmHxPydx";
		String accessToken = "84065249-kiFNccG1vUBdv2lV4Qe195GRyOWdHBC34gpwq7zNQ";
		String accessTokenSecret = "mLGYuGynhTjhDIPNac4APw3lgYTxGwEahrIwbjZDDabxy";
		String[] filters = { "hadoop", "spark", "iot", "ml", "machinelearning" };
		tweetsStream.startTwitterStreaming(consumerKey, consumerSecret, accessToken, accessTokenSecret, filters);

	}

}
