package com.devinline.spark.SparkSample

import org.apache.spark.rdd.RDD

class RddConvertor {
    def toRDD(raw: RDD[String]): RDD[User] =
    raw
      .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
      .map { el =>
        val arr = el.split(",")
        User(arr(0).toInt, arr(1), arr(2), arr(3), arr(4));
      }   
}

case class User(Id: Int, Name: String, number: String, created_at:String, updated_at:String);