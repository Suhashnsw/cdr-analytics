package com.devinline.spark.SparkSample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Sales {
  def main(args: Array[String]) = {
 
    val salesReader = new SalesCSVReader("/home/nelum/Exercises/sales1.csv")
    var statistics = new SalesStatisticsComputer(salesReader)

    //Start the Spark context
    val conf = new SparkConf()
          .setAppName("Writing Int to File")
          .setMaster("local")
    val sc = new SparkContext(conf)
    val num = statistics.getTotalNumberOfSales()
    
    val avgPrices = statistics.getAvgSalePricesGroupedByState()
    val stat = sc.parallelize(avgPrices.toSeq)
    
    stat.map(line => line)
        .saveAsTextFile("Output.txt")
        
    val statByDay = statistics.getNumberOfSalesGroupedByDay()
    val stat2 = sc.parallelize(statByDay.toSeq)
    
     stat2.map(line => line)
         .saveAsTextFile("Output1.txt")  
         
    val statByProvince = statistics.getTotalNumberAndPriceOfSalesMadeAbroad()
    val stat3 = sc.parallelize(statByProvince.productIterator.toList)
    
     stat3.map(line => line)
         .saveAsTextFile("Output2.txt")           
      
    sc.stop()
  }
}