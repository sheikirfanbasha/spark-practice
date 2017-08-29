package com.irfan.services

import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext._;
import org.apache.spark._;
//import org.apache.spark.sql.SparkSession;
object WordCount{
	def countWords(){
		/*
		Parameters to create spark context are:
		1. Spark host machine URL
		2. Name of the application
		3. Home directory of spark
		4. Don't know <To be filled>
		5. Environment for spark
		6. Configuration for worker nodes
		*/
		val sc = new SparkContext("local", "WordCount");
		//val spark = SparkSession.builder.appName("Simple Application").getOrCreate();
		val input = sc.textFile("input.txt");
		//val input = spark.read.textFile("input.txt").cache();
		val count = input.flatMap(line => line.split(" "))
		.map(word => (word, 1))
		.reduceByKey(_ + _);
		count.saveAsTextFile("output.txt");
		sc.stop();
		System.out.println("Successfully proceed word count!");
	}
}