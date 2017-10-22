package com.devinline.spark.SparkSample

/**
  * Trait responsible for reading/loading [[Sale]].
  *
  * @author Nelum Weerakoon
  */

trait SalesReader {
  /**
  * @return A [[Seq]] containing all the sales.
  */
  def readSales(): Seq[Sale]
}

case class Sale(product: String, transaction_date: String, quantity: Int, price: Double, state: String, province: String)