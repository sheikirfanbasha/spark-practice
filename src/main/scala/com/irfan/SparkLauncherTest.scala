package com.irfan;
import org.apache.spark.launcher.SparkLauncher;

object SparkLauncherTest{
	def main(args : Array[String]){
		System.out.println("Hello from SparkLauncher!!!");
		val spark = new SparkLauncher()
		  //.setSparkHome("C:/spark-1.6.0-bin-hadoop2.4")
		  .setVerbose(true)
		  .setAppResource("/Users/irfan/Personal/Project/spark-practice/spark-practice_2.11-1.0.jar")
		  .setMainClass("com.irfan.helloWorld")
		  .setMaster("local")
		  .setSparkHome("/usr/local/spark")
		  .startApplication();

		while (spark.getState.toString != "FINISHED") {

		    println (spark.getState)

		    Thread.sleep(1000)
		}
		spark.stop();
	}
}