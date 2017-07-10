package org.sparktraining.sparkconfig;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkConfigInit {

	public SparkConf initilizedSparkConfig(String master, String appname) {

		SparkConf sparkconf = new SparkConf().setMaster(master).setAppName(appname);

		return sparkconf;
	}

	public SparkSession initilizedSparkSession(String master, String appname) {

		SparkSession sparksession = SparkSession.builder().master(master).appName(appname).enableHiveSupport()
				.getOrCreate();

		return sparksession;
	}

	public SparkSession initilizedSparkSessionwithoutHive(String master, String appname) {

		SparkSession sparksession = SparkSession.builder().master(master).appName(appname).getOrCreate();

		return sparksession;
	}

}
