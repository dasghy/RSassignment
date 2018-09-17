package com.hadoop.sparkApp;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

/*
 * Write Spark java jobs to read all the json files from the asset.zip and vuln.zip and 
 * combine all the data based on the QID from both json and store them in unified format (Parquet).
 */
public class SimpleApp {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("Simple Application").config("spark.master", "local")
				.getOrCreate();
		Dataset<Row> assetData = spark.read().option("multiline", true)
				.json("D:/DataEngineerAssignment/asset/*.json").cache();
		assetData.createOrReplaceTempView("asset");
		Dataset<Row> vulnData = spark.read().option("multiline", true)
				.json("D:/DataEngineerAssignment/vuln/*.json").cache();
		vulnData.createOrReplaceTempView("vuln");

		spark.sql(
				"SELECT asset.*,vuln.* from asset JOIN vuln ON asset.QID == vuln.QID")
				.write().format("parquet")
				.save("D:/DataEngineerAssignment/finalData");

		spark.stop();
	}
}