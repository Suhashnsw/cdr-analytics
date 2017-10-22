package com.devinline.spark.SparkSample

import scala.io.Source

class SalesCSVReader(val fileName: String) extends SalesReader {
  
  override def readSales(): Seq[Sale] = {
    for {
      line <- Source.fromFile(fileName).getLines().drop(1).toVector
      values = line.split(",").map(_.trim)
    } yield Sale(values(0), values(1), values(2).toInt, values(3).toDouble, values(4), values(5))
  }
  
}