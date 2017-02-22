package main

import org.apache.spark.sql.{functions,Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.ArrayType
import scala.collection.mutable.{WrappedArray, ListBuffer}


object JsonPractice extends App {
  val conf = new org.apache.spark.SparkConf()
      .setAppName("ParamsJson")
      .setMaster("local")
  val sc = new org.apache.spark.SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val paramsSchema = sqlContext.read.json("params.json")

//  root
// |-- query: string (nullable = true)
// |-- tablist: array (nullable = true)
// |    |-- element: struct (containsNull = true)
// |    |    |-- dir: string (nullable = true)
// |    |    |-- tabname: string (nullable = true)
// |-- target: string (nullable = true)
// |-- targetOutput: string (nullable = true)
  
  
//  paramsSchema

//  val dataSchema = paramsSchema.
  //  println(dataSchema.apply(0))
//  println(dataSchema.apply(0).apply(0))
//  print(dataSchema.apply(0).getList(1))
  val dataParams = paramsSchema.toDF().first()
//  print(rdd.schema)
//  print(dataParams.getAs("tablist").asInstanceOf[WrappedArray[Row]].apply(0).getAs("tabname"))
  
  
  case class TabList(tabName: String, directory: String)
  case class Params(target: String, targetOutput: String, tabList: ListBuffer[TabList], query: String )
  
  val tabLists = dataParams.getAs("tablist").asInstanceOf[WrappedArray[Row]]
  val target =  dataParams.getAs[String]("target")
  val targetOutput =  dataParams.getAs[String]("targetOutput")
  val query =  dataParams.getAs[String]("query")

  var tabList = new ListBuffer[TabList]()
  
  for(x <- 0 to tabLists.size - 1){
      val newTab = new TabList(
          tabLists.apply(x).getAs("tabname"),
          tabLists.apply(x).getAs("dir")
      )
      
      tabList += newTab
  }
  
  val paramRow = new Params(target, targetOutput, tabList, query)
  

  
//  val tabListContent = TabList()
//  val paramContent = 
  
  
  
}