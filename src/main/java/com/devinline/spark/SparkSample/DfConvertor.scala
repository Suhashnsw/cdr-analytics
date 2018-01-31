package com.devinline.spark.SparkSample

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, TypedColumn}
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.aggregate.TypedAverage
import org.apache.spark.sql.SparkSession

class DfConvertor(val spark: SparkSession) extends java.io.Serializable{
  
  def userRow(line: List[String]): Row = {
    Row(line(0).toInt, line(1), line(2), line(3), line(4))
  }
  
  def callRow(line: List[String]): Row = {
    Row(line(0).toInt, line(1).toInt, line(2), line(3).toInt,
        line(4), line(5), line(6), line(7).toInt, line(8), line(9), line(10))
  }
  
  def messageRow(line: List[String]): Row = {
    Row(line(0).toInt, line(1).toInt, line(2), line(3).toInt,
        line(4), line(5), line(6), line(7).toInt, line(8), line(9), line(10))
  }
  
  def cellTowerRow(line: List[String]): Row = {
    Row(line(0).toInt, line(1).toInt, line(2).toInt,
        line(3), line(4), line(5), line(6), line(7))
  }
  
   def callRecordRow(line: List[String]): Row = {
    Row(line(0).toInt, line(1), line(2),
        line(3), line(4), line(5), line(6), line(7))
  }
   
    def userLocationRow(line: List[String]): Row = {
    Row(line(0), line(1), line(2).toInt,
        line(3).toInt)
  }
    
    def userHomeWorkLocationRow(line: List[String]): Row = {
    Row(line(0), line(1), line(2),
        line(3).toInt,line(4).toInt)
   
  }
    
         
    def towerNameRow(line: List[String]): Row = {
    Row(line(0).toInt, line(1), line(2),
        line(3),line(4),line(5),line(6),line(7),line(8),line(9),line(10),line(11))
    }
    
    def userHomeWorkLoc(line: List[String]): Row = {
      Row(line(0), line(1).toInt, line(2).toInt)
    }
  
  def toUserDF(raw: RDD[String]): DataFrame = {
    val headerColumns = raw.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfUserSchema(headerColumns)

    val data =
      raw
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(userRow)

    spark.createDataFrame(data, schema)
  }
  
  def toCallDF(raw: RDD[String]): DataFrame = {
    val headerColumns = raw.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfCallSchema(headerColumns)

    val data =
      raw
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(callRow)

    spark.createDataFrame(data, schema)
  }
  
  def toMessageDF(raw: RDD[String]): DataFrame = {
    val headerColumns = raw.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfCallSchema(headerColumns)

    val data =
      raw
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(messageRow)

    spark.createDataFrame(data, schema)
  }
  
  def toCellTowerDF(raw: RDD[String]): DataFrame = {
    val headerColumns = raw.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfCellTowerSchema(headerColumns)

    val data =
      raw
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(cellTowerRow)

    spark.createDataFrame(data, schema)
  }
  
    def toCallRecordDF(raw: RDD[String]): DataFrame = {
    val headerColumns = raw.first().split("\\|").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfCallRecordSchema(headerColumns)

    val data =
      raw
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split("\\|").to[List])
        .map(callRecordRow)

    spark.createDataFrame(data, schema)
  }
    
   def toUserLocationsDF(raw: RDD[String]): DataFrame = {
    val headerColumns = raw.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfUserLocationsSchema(headerColumns)

    val data =
      raw
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(userLocationRow)

    spark.createDataFrame(data, schema)
  }
   
    def toTowerNameDF(raw: RDD[String]): DataFrame = {
    val headerColumns = raw.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfTowerNameSchema(headerColumns)

    val data =
      raw
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(towerNameRow)

    spark.createDataFrame(data, schema)
  }
   
    
       def toUserHomeWorkLocDF(raw: RDD[String]): DataFrame = {
    val headerColumns = raw.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfUserHomeWorkLocSchema(headerColumns)

    val data =
      raw
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(userHomeWorkLoc)

    spark.createDataFrame(data, schema)
  }
   
    
   def toUserHomeWorkLocationsDF(raw: RDD[String]): DataFrame = {
    val headerColumns = raw.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfUserHomeWorkLocationSchema(headerColumns)

    val data =
      raw
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(userHomeWorkLocationRow)

    spark.createDataFrame(data, schema)
  }
   
   
  def dfUserSchema(columnNames: List[String]): StructType = {
    StructType(
      Seq(
        StructField(name = "id", dataType = IntegerType, nullable = false),
        StructField(name = "name", dataType = StringType, nullable = false),
        StructField(name = "number", dataType = StringType, nullable = false),
        StructField(name = "created_at", dataType = StringType, nullable = false),
        StructField(name = "updated_at", dataType = StringType, nullable = false)
      )
    )
  }
  
  def dfCallSchema(columnNames: List[String]): StructType = {
    StructType(
      Seq(
        StructField(name = "id", dataType = IntegerType, nullable = false),
        StructField(name = "user_id", dataType = IntegerType, nullable = false),
        StructField(name = "other_id", dataType = StringType, nullable = true),
        StructField(name = "interaction", dataType = IntegerType, nullable = false),
        StructField(name = "timestamp", dataType = StringType, nullable = false),
        StructField(name = "number", dataType = StringType, nullable = false),
        StructField(name = "call_timestamp", dataType = StringType, nullable = false),
        StructField(name = "duration", dataType = IntegerType, nullable = false),
        StructField(name = "direction", dataType = StringType, nullable = false),
        StructField(name = "created_at", dataType = StringType, nullable = false),
        StructField(name = "updated_at", dataType = StringType, nullable = false)
      )
    )
  }
  
  def dfCellTowerSchema(columnNames: List[String]): StructType = {
    StructType(
      Seq(
        StructField(name = "id", dataType = IntegerType, nullable = false),
        StructField(name = "user_id", dataType = IntegerType, nullable = false),
        StructField(name = "interaction", dataType = IntegerType, nullable = false),
        StructField(name = "timestamp", dataType = StringType, nullable = false),
        StructField(name = "cellid", dataType = StringType, nullable = false),
        StructField(name = "lac", dataType = StringType, nullable = false),
        StructField(name = "created_at", dataType = StringType, nullable = false),
        StructField(name = "updated_at", dataType = StringType, nullable = true)
      )
    )
  }
  
  def dfMessageSchema(columnNames: List[String]): StructType = {
    StructType(
      Seq(
        StructField(name = "id", dataType = IntegerType, nullable = false),
        StructField(name = "user_id", dataType = IntegerType, nullable = false),
        StructField(name = "other_id", dataType = StringType, nullable = true),
        StructField(name = "interaction", dataType = IntegerType, nullable = false),
        StructField(name = "timestamp", dataType = StringType, nullable = false),
        StructField(name = "number", dataType = StringType, nullable = false),
        StructField(name = "message_timestamp", dataType = StringType, nullable = false),
        StructField(name = "length", dataType = IntegerType, nullable = false),
        StructField(name = "direction", dataType = StringType, nullable = false),
        StructField(name = "created_at", dataType = StringType, nullable = false),
        StructField(name = "updated_at", dataType = StringType, nullable = false)
      )
    )
  }
  
    def dfCallRecordSchema(columnNames: List[String]): StructType = {
    StructType(
      Seq(
        StructField(name = "key", dataType = IntegerType, nullable = false),
        StructField(name = "device", dataType = StringType, nullable = false),
        StructField(name = "anumber", dataType = StringType, nullable = true),
        StructField(name = "other_number", dataType = StringType, nullable = false),
        StructField(name = "cell_id", dataType = StringType, nullable = false),
        StructField(name = "call_time", dataType = StringType, nullable = false),
        StructField(name = "duration", dataType = StringType, nullable = false),
        StructField(name = "call_type", dataType = StringType, nullable = false)
      )
    )
    
    
  }
    
   def dfUserLocationsSchema(columnNames: List[String]): StructType = {
    StructType(
      Seq(
        StructField(name = "cellid", dataType = StringType, nullable = false),
        StructField(name = "userid", dataType = StringType, nullable = false),
        StructField(name = "homehits", dataType = IntegerType, nullable = true),
        StructField(name = "workhits", dataType = IntegerType, nullable = false)
 
      )
    )
  }
   
    def dfUserHomeWorkLocationSchema(columnNames: List[String]): StructType = {
    StructType(
      Seq(
        StructField(name = "user", dataType = StringType, nullable = false),
        StructField(name = "home_cellId", dataType = StringType, nullable = false),
        StructField(name = "work_cellId", dataType = StringType, nullable = false),
        StructField(name = "max_homehits", dataType = IntegerType, nullable = true),
        StructField(name = "Max_workhits", dataType = IntegerType, nullable = false)
 
      )
    )
  }
   
      def dfTowerNameSchema(columnNames: List[String]): StructType = {
    StructType(
      Seq(
        StructField(name = "cellid", dataType = IntegerType, nullable = false),
        StructField(name = "siteid", dataType = StringType, nullable = false),
        StructField(name = "longitude", dataType = StringType, nullable = false),
        StructField(name = "latitude", dataType = StringType, nullable = true),
        StructField(name = "azimuth", dataType = StringType, nullable = false),
        StructField(name = "max_cell_d", dataType = StringType, nullable = false),
        StructField(name = "DSD_Code4", dataType = StringType, nullable = false),
        StructField(name = "Province_N", dataType = StringType, nullable = true),
        StructField(name = "District_N", dataType = StringType, nullable = false),
        StructField(name = "DS_N", dataType = StringType, nullable = false),
        StructField(name = "index", dataType = StringType, nullable = true),
        StructField(name = "1k_cell", dataType = StringType, nullable = false)
 
        //cellid,siteid,longitude,latitude,dfUserHomeWorkLocSchema
        //azimuth,max_cell_d,DSD_Code4,Province_N,District_N,DS_N,index,1k_cell
      )
    )
  }
    
         def dfUserHomeWorkLocSchema(columnNames: List[String]): StructType = {
    StructType(
      Seq(
        StructField(name = "SUBSCRIBER_ID", dataType = StringType, nullable = false),
        StructField(name = "HOME_CELL_ID", dataType = IntegerType, nullable = false),
        StructField(name = "WORK_CELL_ID", dataType = IntegerType, nullable = false)
    
 
        
      )
    )
  }
  
}