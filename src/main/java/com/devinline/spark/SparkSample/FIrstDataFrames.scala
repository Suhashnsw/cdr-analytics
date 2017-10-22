package com.devinline.spark.SparkSample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FIrstDataFrames {
  val conf = new SparkConf() 
              .setMaster("local[*]") 
              .setAppName("Working with DataFrames")
  
  val sc = new SparkContext(conf)

}