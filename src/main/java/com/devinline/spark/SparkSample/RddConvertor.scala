package com.devinline.spark.SparkSample

import org.apache.spark.rdd.RDD

class RddConvertor {    
    def toUserRDD(raw: RDD[String]): RDD[User] =
    raw
      .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
      .map { el =>
        val arr = el.split(",")
        User(arr(0).toInt, arr(1), arr(2), arr(3), arr(4));
      }
    
    def toCallRDD(raw: RDD[String]): RDD[Call] =
    raw
      .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
      .map { el =>
        val arr = el.split(",")
        Call(arr(0).toInt, arr(1).toInt, arr(2), arr(3).toInt,
            arr(4), arr(5), arr(6), arr(7).toInt, arr(8), arr(9), arr(10));
      }
    
    def toMessageRDD(raw: RDD[String]): RDD[Message] =
    raw
      .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
      .map { el =>
        val arr = el.split(",")
        Message(arr(0).toInt, arr(1).toInt, arr(2), arr(3).toInt,
            arr(4), arr(5), arr(6), arr(7).toInt, arr(8), arr(9), arr(10));
      }
    
    def toCellTowerRDD(raw: RDD[String]): RDD[CellTower] =
    raw
      .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
      .map { el =>
        val arr = el.split(",")
        CellTower(arr(0).toInt, arr(1).toInt, arr(2).toInt,
            arr(3), arr(4), arr(5), arr(6),arr(7));
      }
    
    def toCAllRecordRDD(raw: RDD[String]): RDD[CAllRecord] =
    raw
      .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
      .map { el =>
        val arr = el.split("|")
        CAllRecord(arr(0).toInt, arr(1), arr(2),
            arr(3), arr(4), arr(5), arr(6),arr(7));
      }
}

case class User(id: Int, Name: String, number: String, created_at:String, updated_at:String);

case class Call(id: Int, user_id: Int, other_id: String, interaction: Int, timestamp :String, 
                number: String, call_timestamp: String, duration: Int, direction: String,
                created_at: String, updated_at: String);

case class Message(id: Int, user_id: Int, other_id: String, interaction: Int, timestamp :String, 
                number: String, message_timestamp: String, length: Int, direction: String,
                created_at: String, updated_at: String);

case class CellTower(id: Int, user_id: Int, interaction: Int, timestamp :String, 
                     cellid: String, lac: String, created_at: String, updated_at: String);

case class CAllRecord(key: Integer, device: String, anumber: String, other_number :String, 
                     cell_id: String, call_time: String, duration: String, call_type: String);