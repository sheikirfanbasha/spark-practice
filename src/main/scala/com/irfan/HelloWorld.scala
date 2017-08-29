package com.irfan
import com.irfan.services._;

object helloWorld{
	def main(args: Array[String]){
		System.out.println("helloWorld");
		WordCount.countWords();
	}
}