package com.devinline.spark.SparkSample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object CSVWordCount {
  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("CsvWordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    //Read some example file to a test RDD
    val test = sc.textFile("/home/nelum/Exercises/csvInput.csv")

    test.distinct()// remove all duplicate key;value pairs
      .map( line => line.split(",")(1) ) // extract the keys
    //  .map( k => (k,1) ) // convert to countable tuples
    //  .reduceByKey(_+_) // count keys
      .saveAsTextFile("output.txt") //Save to a text file
      
    //Stop the Spark context
    sc.stop
  }
}