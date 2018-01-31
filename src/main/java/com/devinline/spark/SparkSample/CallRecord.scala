package com.devinline.spark.SparkSample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset, Row, TypedColumn}
import org.apache.spark.sql.functions._
import collection.mutable.HashMap
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions.col
import scala.io.Source
import java.io.File
import java.io.PrintWriter

object CallRecord {
  
    val spark: SparkSession = SparkSession.builder()
                                        .appName("Mobile Usage")
                                        .config("spark.master", "local")
                                        .getOrCreate();
   // val writer = new PrintWriter(new File("/home/nelum/learnAsiaData/processed_data.csv"))
   //   val writer = new PrintWriter(new File("/home/nelum/learnAsiaData/home_work_locs.csv"))
    val writer = new PrintWriter(new File("/home/nelum/learnAsiaData/test.csv"))
    
   def main(args: Array[String]){
   /*val callRecordDf = createCallRecordDataFrame("/home/nelum/learnAsiaData/voice_sample.csv");
    // callRecordDf.show();
   groupUsersByCellId(callRecordDf);*/
    
  // val userLocDf = createUserLocationDataFrame("/home/nelum/learnAsiaData/processed_data.csv")
  // identifyHomeAndWorkLocations(userLocDf);
   
   val userHomeWorkLocDf = createUserHomeWorkLocationDataFrame("/home/nelum/learnAsiaData/home_work_locs.csv")
   processUserHomeAndWorkLocations(userHomeWorkLocDf)
   
   spark.close();
   writer.close();
  }
  
    def processUserHomeAndWorkLocations(df:DataFrame)
  {
    df.rdd.groupBy(r => r(1)).foreach(f=> printUserHomeLocation ("Home", f._1.toString(),f._2 ))
    df.rdd.groupBy(r => r(2)).foreach(f=> printUserHomeLocation ("Work", f._1.toString(),f._2 ))
       
  }
  
  def printUserHomeLocation(homeOrWork:String,cellId:String, rows:Iterable[Row])
  {
    print(homeOrWork + " Cell ID:" + cellId);
    rows.groupBy(u=>u(0)).foreach(r=> print("\t" + r._1.toString()))
    print("\n")
  }
   def groupUsersByCellId(df :DataFrame) {
   
    
     df.rdd.groupBy(r => r(4))
     .foreach(
       f=> f._2.
           foreach(
            u => identifyUserLocations( f._1.toString(),  
                if (u(0)== 1) f._2.groupBy(u => u(3)) else f._2.groupBy(u => u(2)) )  )
       );
   }
   
    def identifyUserLocations(cell:String, map :Map[Any,Iterable[Row]]){
    
       map.foreach(i => countNumberOfDays(cell, i._1.toString(),i._2))
    }
    
     def identifyHomeAndWorkLocations(df:DataFrame)
  {
    df.rdd.groupBy(f=>f(1)).foreach(r=> findHomeAndLocationCellId(r._1.toString(),r._2))
    
  }
  
  def findHomeAndLocationCellId(user:String, rows:Iterable[Row])
  {
    var maxNoOfHomeHits  = 0;
    var maxNoOfWorkHits = 0;
    var homeCellId =""
    var workCellId =""
    
    for(row <- rows){
    
      if(maxNoOfHomeHits <= row(2).toString().toInt)
      {
        maxNoOfHomeHits = row(2).toString().toInt
        homeCellId =row(0).toString()
      }
      
      if(maxNoOfWorkHits <= row(3).toString().toInt)
      {
        maxNoOfWorkHits = row(3).toString().toInt
        workCellId =row(0).toString()
      }
    }
    
    if(homeCellId.toString().toInt != workCellId.toString().toInt)
    {
     writer.write(user+ "," + homeCellId +"," +workCellId+","+ maxNoOfHomeHits.toString()+ ","+maxNoOfWorkHits.toString() + "\n");
    }
  }
    
    def countNumberOfDays(cell:String, user:String, rows:Iterable[Row]){
      var home = 0;
      var work = 0;
      var homeList = scala.collection.mutable.SortedSet[String]();
      var dayList =  scala.collection.mutable.SortedSet[String]();
    
    
      for(row <- rows){
        val time = row(5).toString().substring(8,10).toInt
        val date = row(5).toString().substring(0, 8)
        
        if (time >= 18 || time <= 7) //Home
        {
          homeList.add(date) ;
        }
        else if( time >= 8 && time <= 17)//Work
        {
          dayList.add(date) ;
        }
      }
    
      home = homeList.size;
      work = dayList.size;
    
     writer.write(cell+ "," + user +"," + home.toString()+ ","+work.toString() + "\n");
  
    }
   
   def addToLocationMap(map :HashMap[String, List[String]], cell: String, user: String)
  {
      //println("cell=" + cell);
     //println("user=" + user);
      var list =  map.getOrElse(cell, List()) ;
      list.++=(List(user)) ;
      //println("size=" + list.size)
      map.put(cell, list);
      //println("mapsize=" + map.size)
  }
   
    def createCallRecordDataFrame(file_path: String) : DataFrame ={
    var csvReader = new UserCsvRedear(spark);
  //  var rddConvertor = new RddConvertor();
    var dfConvertor = new DfConvertor(spark);
    
    val rawRDD = csvReader.readFile(file_path);
    
 //   val initRDD = rddConvertor.toCAllRecordRDD(rawRDD);
    val initDf = dfConvertor.toCallRecordDF(rawRDD);
    return initDf
    
    //val finalDf = userGrouped(userDf);
    
    //finalDf.show();
  }
  
   def createUserLocationDataFrame(file_path: String) : DataFrame ={
     var csvReader = new UserCsvRedear(spark);
     var dfConvertor = new DfConvertor(spark);
    
     val rawRDD = csvReader.readFile(file_path);
  
     return dfConvertor.toUserLocationsDF(rawRDD)
    
    //val finalDf = userGrouped(userDf);
    
    //finalDf.show();
  }
   
   def createUserHomeWorkLocationDataFrame(file_path: String) : DataFrame ={
     var csvReader = new UserCsvRedear(spark);
     var dfConvertor = new DfConvertor(spark);
    
     val rawRDD = csvReader.readFile(file_path);
  
     return dfConvertor.toUserHomeWorkLocationsDF(rawRDD)
    
    //val finalDf = userGrouped(userDf);
    
    //finalDf.show();
  }
}