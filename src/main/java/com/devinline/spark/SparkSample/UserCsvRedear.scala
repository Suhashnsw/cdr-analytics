package com.devinline.spark.SparkSample

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.nio.file.Paths
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class UserCsvRedear(val spark: SparkSession) extends UserReader{
 
  override def fsPath(resource: String): String =
    Paths.get(resource).toString   

  
  override def readFile(resource: String): RDD[String] =
    spark.sparkContext.textFile(fsPath(resource))
    
}