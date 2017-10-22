package com.devinline.spark.SparkSample

class SalesStatisticsComputer(val salesReader: SalesReader) {
  
  val sales = salesReader.readSales
  
  /**
    * @return The number of sales
    */
  def getTotalNumberOfSales(): Int = sales size
  
    /**
    * Creates and returns a [[Map]] where the key is a state and the value is the
    * average sale price for all sales made using that state.
    *
    * @return The map with the data grouped by state.
    */
  def getAvgSalePricesGroupedByState(): Map[String, Double] = {
    def avg(salesOfAState: Seq[Sale]): Double =
      salesOfAState.map(_.price).sum / salesOfAState.size
 
    sales.groupBy(_.state).mapValues(avg(_))
  }
  
 
  /**
    * Creates and returns a [[Map]] where the key is a given day in the format month/day
    * and the value is the number of sales made in the day.
    *
    * @return The map with the data grouped by transaction_date.
    */
  def getNumberOfSalesGroupedByDay(): Map[String, Int] = {
    def extractDay(sale: Sale): String = {
      val parts = sale.transaction_date.split("/")
      parts(0) + "/" + parts(1)
    }
 
    sales.groupBy(extractDay(_)).mapValues(_.length)
  }
  
 
  /**
    * @return A tuple where the first value is the number of sales made out of USA and
    *         the second value is the average price of these sales.
    */
  def getTotalNumberAndPriceOfSalesMadeAbroad(): (Int, Double) = {
    val filtered = sales.filter(_.province != "West")
    (filtered.size, filtered.map(_.price).sum)
  }  
}