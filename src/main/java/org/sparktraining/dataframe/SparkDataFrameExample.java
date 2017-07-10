package org.sparktraining.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.sparktraining.sparkconfig.SparkConfigInit;

public class SparkDataFrameExample {

	SparkConfigInit sparkConfig = new SparkConfigInit();

	public void mysqlConnectExample() {

		SparkSession spark = sparkConfig.initilizedSparkSessionwithoutHive("local[2]", "Spark DataFrame");
		Dataset<Row> df = spark.read().format("jdbc").option("url", "jdbc:mysql://localhost/awsimmersion")
				.option("driver", "com.mysql.jdbc.Driver").option("dbtable", "company_announcement").option("user", "root")
				.option("password", "eminent").load();
		
		
		df.show();
	}
	
	
	public static void main(String args[]){
		
		SparkDataFrameExample sparkSql = new SparkDataFrameExample();
		
		sparkSql.mysqlConnectExample();
		
	}

}
