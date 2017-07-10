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

public class SparkStreamingTwitter implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	SparkConfigInit sparkConfig = new SparkConfigInit();

	public <U> void startTwitterStreaming(String consumerKey, String consumerSecret, String accessToken,
			String accessTokenSecret, String[] filters) throws InterruptedException {

		// Set the system properties so that Twitter4j library used by Twitter
		// stream
		// can use them to generate OAuth credentials
		/*System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
		System.setProperty("twitter4j.oauth.accessToken", accessToken);
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);*/
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setOAuthConsumerKey(consumerKey); // Change the Key value, Token etc. with your actual token value
		cb.setOAuthConsumerSecret(consumerSecret);
		cb.setOAuthAccessToken(accessToken);
		cb.setOAuthAccessTokenSecret(accessTokenSecret);
		
		
		Authorization auth = AuthorizationFactory.getInstance(cb.build());

		
		SparkConf sparkConf = new SparkConf().setAppName("Tweets Android").setMaster("local[2]").set("spark.serializer",KryoSerializer.class.getName());
		
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, auth,filters);
		
		
		stream.print();
		
		    	    
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
