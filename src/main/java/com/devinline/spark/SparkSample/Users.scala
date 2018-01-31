package com.devinline.spark.SparkSample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset, Row, TypedColumn}
import org.apache.spark.sql.functions._
import collection.mutable.HashMap
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions.col

object Users {
  val spark: SparkSession = SparkSession.builder()
                                        .appName("Mobile Usage")
                                        .config("spark.master", "local")
                                        .getOrCreate();
  
  def main(args: Array[String]){
   val userDf = createUserDataFrame("/home/nelum/NoDoBoFiles/Users.csv");
   val cellTowerDf = createCellTowerDataFrame("/home/nelum/NoDoBoFiles/cell_towers.csv");
   val callsDf = createCallDataFrame("/home/nelum/NoDoBoFiles/calls.csv");
   val messagesDf = createMessageDataFrame("/home/nelum/NoDoBoFiles/Messages.csv");
   //val presencesDf = createDataFrame("/home/nelum/NoDoBoFiles/presences.csv");
    
   userDf.show();
   callsDf.show();
   cellTowerDf.show();
   messagesDf.show();
  //  identifyUserLocations(callsDf,cellTowerDf);

    spark.close();
  }
  
  def createUserDataFrame(file_path: String): DataFrame = {
    var csvReader = new UserCsvRedear(spark);
    var rddConvertor = new RddConvertor();
    var dfConvertor = new DfConvertor(spark);
    val rawRDD = csvReader.readFile(file_path);
    
    val initRDD = rddConvertor.toUserRDD(rawRDD);
    val initDf = dfConvertor.toUserDF(rawRDD);
    return initDf
    
    //val finalDf = userGrouped(userDf);
    
    //finalDf.show();
  }
  
  def createCallDataFrame(file_path: String) : DataFrame ={
    var csvReader = new UserCsvRedear(spark);
    var rddConvertor = new RddConvertor();
    var dfConvertor = new DfConvertor(spark);
    
    val rawRDD = csvReader.readFile(file_path);
    
    val initRDD = rddConvertor.toCallRDD(rawRDD);
    val initDf = dfConvertor.toCallDF(rawRDD);
    return initDf
    
    //val finalDf = userGrouped(userDf);
    
    //finalDf.show();
  }
  
  def createMessageDataFrame(file_path: String) : DataFrame ={
    var csvReader = new UserCsvRedear(spark);
    var rddConvertor = new RddConvertor();
    var dfConvertor = new DfConvertor(spark);
    
    val rawRDD = csvReader.readFile(file_path);
    
    val initRDD = rddConvertor.toMessageRDD(rawRDD);
    val initDf = dfConvertor.toMessageDF(rawRDD);
    return initDf
    
    //val finalDf = userGrouped(userDf);
    
    //finalDf.show();
  }
  
  def createCellTowerDataFrame(file_path: String) : DataFrame ={
    var csvReader = new UserCsvRedear(spark);
    var rddConvertor = new RddConvertor();
    var dfConvertor = new DfConvertor(spark);
    
    val rawRDD = csvReader.readFile(file_path);
    
    val initRDD = rddConvertor.toCellTowerRDD(rawRDD);
    val initDf = dfConvertor.toCellTowerDF(rawRDD);
    return initDf
    
    //val finalDf = userGrouped(userDf);
    
    //finalDf.show();
  }
  
  def userGrouped(data: DataFrame): DataFrame = {     
      data.select("*")
          .where(data("number") === "076%")
  }
  
  def identifyUserLocations(df_calls: DataFrame, df_cellTowers: DataFrame){    
    var cluster = 1;  
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:MM")

    
   val user_location_df = df_calls
                              .join(df_cellTowers, "interaction")
                              .select(
                                  df_calls("user_id") as "user_id", 
                                  df_cellTowers("cell_id") as "cellid",
                                  df_cellTowers("lac") as "location")
                              //.withColumn("date", col("df_cellTowers.timestamp").cast(DateType))
                              //.withColumn("hour",from_unixtime(unix_timestamp(col("df_cellTowers.timestamp")).g, "YYYY-MM-dd HH:mm:ss"))
    
    /*,
                                  df_cellTowers("interaction") as "interaction",
                                  
                                  df_calls("duration") as "duration"*/
user_location_df.write.format("com.databricks.spark.csv").save("locations.csv")                                                                    

  }
  
    
}