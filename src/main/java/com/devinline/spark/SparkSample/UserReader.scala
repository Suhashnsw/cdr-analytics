package com.devinline.spark.SparkSample

import org.apache.spark.rdd.RDD

/**
  * Trait responsible for reading/loading [[Sale]].
  * 
  */
trait UserReader {
  /**
  * @return A [[Seq]] containing all the sales.
  */
  def fsPath(resource: String): String;
  
  def readUsers(resource: String): RDD[String];
}