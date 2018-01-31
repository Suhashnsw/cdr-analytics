package com.devinline.spark.SparkSample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset, Row, TypedColumn}
import org.apache.spark.sql.functions._
import collection.mutable.HashMap
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions.col
import util.control.Breaks._
import scala.io.Source
import java.io.File
import java.io.PrintWriter


object LocationCluster {
  
  val spark: SparkSession = SparkSession.builder()
                                        .appName("Mobile Usage")
                                        .config("spark.master", "local")
                                        .getOrCreate();
  
   val writer = new PrintWriter(new File("/home/nelum/learnAsiaData/regions_with_districts.csv" ))
  
   def main(args: Array[String]){
     
    
      writer.write("District"+ "," + "Region" + "\n");
     val cellTowerDf = createCellTowerNameDataFrame("/home/nelum/learnAsiaData/indexed_towers_western.csv");
     val distinct_regions = cellTowerDf.
                             select(cellTowerDf("District_N"), cellTowerDf("DS_N")).
                             distinct() ;
     
     distinct_regions.foreach(f=> writeToFile(f(0).toString(), f(1).toString()))
     
   //  distinct_regions.show()
     
   //  val cellIdArray =cellTowerDf.select("cellid").rdd.map(r => r(0)).collect().distinct;
     
   //  println(cellIdArray.mkString(" "))
     val homeLocDf =  createUserHomeLocDataFrame("/home/nelum/learnAsiaData/hw_loc_western.csv");
     
     val user_home_location_df = homeLocDf
                              .join(cellTowerDf, homeLocDf.col("HOME_CELL_ID") === cellTowerDf.col("cellid"))
                              .select(
                                  homeLocDf("WORK_CELL_ID") as "WORK_CELL_ID", 
                                  homeLocDf("HOME_CELL_ID") as "HOME_CELL_ID",
                                  homeLocDf("SUBSCRIBER_ID") as "SUBSCRIBER_ID",
                                  cellTowerDf("1k_cell") as "Home_1k_cell",
                                  cellTowerDf("DS_N") as "Home_DS_N")
                              
     val user_home_work_loc = user_home_location_df
                              .join(cellTowerDf, user_home_location_df.col("WORK_CELL_ID") === cellTowerDf.col("cellid"))
                              
                              .select(
                                  user_home_location_df("WORK_CELL_ID") as "WORK_CELL_ID", 
                                  user_home_location_df("HOME_CELL_ID") as "HOME_CELL_ID",
                                  user_home_location_df("SUBSCRIBER_ID") as "SUBSCRIBER_ID",
                                  user_home_location_df("Home_1k_cell") as "Home_1k_cell",
                                  user_home_location_df("Home_DS_N") as "Home_DS_N",
                                  cellTowerDf("1k_cell") as "Work_1k_cell",
                                  cellTowerDf("DS_N") as "Work_DS_N",
                                  concat_ws("_",cellTowerDf("1k_cell"),user_home_location_df("Home_1k_cell")) as "KEY",
                                  concat_ws("_",user_home_location_df("Home_DS_N"),cellTowerDf("DS_N")) as "Route_Key"
                                  
                              )
                                  
                               
                                  
  
val user_home_work_loc_filterd =  user_home_work_loc.where(user_home_work_loc.col("Work_1k_cell") !== user_home_work_loc.col("Home_1k_cell") )

//user_home_work_loc_filterd.rdd.groupBy(r => r(8)).foreach(f=>writeToGroupedFiles(f._1.toString(),f._2));

//user_home_location_df.show()
  //   user_work_location_df.show()
     
     
  //   user_home_work_loc_filterd.show()
  //  cellTowerDf.show();
   //  homeLocDf.show() ;
     
    //for (cell <- cellIdArray)
   // {
      
      
   // }
      writer.close();
     spark.close();
   }
   
    def writeToFile(val1:String, val2:String)
  {
    writer.write(val1+ "," + val2+ "\n");
  }
   
   def writeToGroupedFiles(key:String, rows:Iterable[Row])
  {
     println("route=" + key + " : " + rows.size.toString())
    
     writer.write(key+ "," + rows.size.toString()+ "\n");
   /* val writer = new PrintWriter(new File("/home/nelum/learnAsiaData/data/" + key +  ".csv"))
    writer.write("WORK_CELL_ID"+ "," + "HOME_CELL_ID" +"," + "SUBSCRIBER_ID"+ ","+"Home_1k_cell"+"," + "Home_DS_N" + "," + "Work_1k_cell"+ ","  + "Work_DS_N" + "\n");
    for(row <- rows){
    
      writer.write(row(0)+ "," + row(1)+"," + row(2)+ ","+row(3)+"," + row(4) + "," + row(5)+ ","  + row(6) + "\n");
      
    }
    writer.close();*/
  
     
  }
   
   def clusterLocations()
   {
     val cellTowerDf = createCellTowerDataFrame("/home/nelum/NoDoBoFiles/cell_towers.csv");
     var cells = extractColumns(cellTowerDf, "cellid");
  //   var locationMap = createLocationClusters(cells);
   }
   
   def extractColumns(df : DataFrame, columnName : String) : Array[Any]=
   {
     return df.select(columnName).rdd.map(r => r(0)).collect() ;
   }
   
   def createLocationClusters(arr : Array[String]) : HashMap[String, List[String]]=
   {
     val locationMap = new HashMap[String, List[String]] ;

     var cellCurrent = arr(0);
     var list = List(cellCurrent)
     locationMap.put(cellCurrent, list);
       
     for (a <- arr)
     {
      if(DistanceWithinRange(cellCurrent,a))
      {
       var l =  locationMap.getOrElse(a, List()) ;
       l.++=(List(a)) ;
       locationMap.put(a, l);
      }
      else
      {
        cellCurrent = null;
        breakable {
          for (al <- arr)
          {
            
            if(DistanceWithinRange(a,al))
            {
              var l =  locationMap.getOrElse(al, List()) ;
              l.++=(List(a)) ;
              locationMap.put(al, l);
              cellCurrent= al
              break;
            }
          }
          
          if(cellCurrent == null)
          {
             var l = List(a)
             locationMap.put(a, l);
             cellCurrent = a;
          }
        }
      }
      
     }
   
     
     return locationMap;
   }
   
  def DistanceWithinRange(from : String, to : String) : Boolean =
  {
    return true;
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
  
  def createCellTowerNameDataFrame(file_path: String) : DataFrame ={
    var csvReader = new UserCsvRedear(spark);
   // var rddConvertor = new RddConvertor();
    var dfConvertor = new DfConvertor(spark);
    
    val rawRDD = csvReader.readFile(file_path);
    
   
    val initDf = dfConvertor.toTowerNameDF(rawRDD);
    return initDf
    
    //val finalDf = userGrouped(userDf);
    
    //finalDf.show();
  }
  
  def createUserHomeLocDataFrame(file_path: String) : DataFrame ={
    var csvReader = new UserCsvRedear(spark);
   // var rddConvertor = new RddConvertor();
    var dfConvertor = new DfConvertor(spark);
    
    val rawRDD = csvReader.readFile(file_path);
    
   
    val initDf = dfConvertor.toUserHomeWorkLocDF(rawRDD);
    return initDf
    
    //val finalDf = userGrouped(userDf);
    
    //finalDf.show();
  }
  
}