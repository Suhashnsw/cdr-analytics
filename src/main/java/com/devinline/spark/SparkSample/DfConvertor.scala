package com.devinline.spark.SparkSample

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, TypedColumn}
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.aggregate.TypedAverage
import org.apache.spark.sql.SparkSession

class DfConvertor(val spark: SparkSession) extends java.io.Serializable{
  
  def row(line: List[String]): Row = {
    Row(line(0).toInt, line(1), line(2), line(3), line(4))
  }
  
  def toDF(raw: RDD[String]): DataFrame = {
    val headerColumns = raw.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)

    val data =
      raw
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

    spark.createDataFrame(data, schema)
  }
  
  def dfSchema(columnNames: List[String]): StructType = {
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
}