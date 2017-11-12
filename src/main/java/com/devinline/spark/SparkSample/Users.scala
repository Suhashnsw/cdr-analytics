package com.devinline.spark.SparkSample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset, Row, TypedColumn}
import org.apache.spark.sql.functions._
import collection.mutable.HashMap
import org.apache.spark.sql.types.DateType

object Users {
  val spark: SparkSession = SparkSession.builder()
                                        .appName("Mobile Usage")
                                        .config("spark.master", "local")
                                        .getOrCreate();
  
  def main(args: Array[String]){
    val userDf = createDataFrame("/home/nelum/NoDoBoFiles/Users.csv");
    val cellTowerDf = createDataFrame("/home/nelum/NoDoBoFiles/cell_towers.csv");
    val callsDf = createDataFrame("/home/nelum/NoDoBoFiles/calls.csv");
    val messagesDf = createDataFrame("/home/nelum/NoDoBoFiles/Messages.csv");
    val presencesDf = createDataFrame("/home/nelum/NoDoBoFiles/presences.csv");

    spark.close();
  }
  
  def createDataFrame(file_path: String): DataFrame = {
    var userReader = new UserCsvRedear(spark);
    var rddConvertor = new RddConvertor();
    var dfConvertor = new DfConvertor(spark);
    
    val rawRDD = userReader.readUsers(file_path);
    val initRDD = rddConvertor.toRDD(rawRDD);
    val initDf = dfConvertor.toDF(rawRDD);
    
    //val finalDf = userGrouped(userDf);
    
    //finalDf.show();
    
    return initDf
  }
  
  def userGrouped(data: DataFrame): DataFrame = {     
      data.select("*")
          .where(data("number") === "076%")
  }
  
  def identifyUserLocations(df_calls: DataFrame, df_cellTowers: DataFrame){    
    var cluster = 1;  
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:MM")

    
    val user_location_df = df_calls
                              .join(df_cellTowers, col("df_users.interaction") === col("df_cellTowers.interaction"), "inner")
                              .select(
                                  col("df_users.user_id") as "user_id", 
                                  col("df_cellTowers.lac") as "location", 
                                  col("df_cellTowers.cellid") as "cellid")
                              .withColumn("date", col("df_cellTowers.timestamp").cast(DateType))
                              //.withColumn("hour",from_unixtime(unix_timestamp(col("df_cellTowers.timestamp")).g, "YYYY-MM-dd HH:mm:ss"))
                                  
  }
  
    
}