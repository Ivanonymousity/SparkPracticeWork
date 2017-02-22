package main

import org.apache.spark._
import org.apache.spark.sql.functions.{explode}
import org.apache.spark.sql.{functions,Row}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column

object practiceJson extends App {
  val conf = new org.apache.spark.SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
  val sc = new org.apache.spark.SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val bakerSchema = sqlContext.read.json("newJson.json")
  bakerSchema.createOrReplaceTempView("students")
  import sqlContext.implicits._
  

  val schools = sqlContext.sql("select schools from students")
  val schoolList = schools.select(explode($"schools")).collect()
  val getData = schoolList.map(x => List(x.apply(0).asInstanceOf[Row].apply(0)))
  
//  println(schoolList)
  
//  println(getData)
//  
//  root
// |-- cities: array (nullable = true)
// |    |-- element: string (containsNull = true)
// |-- name: string (nullable = true)
// |-- schools: array (nullable = true)
// |    |-- element: struct (containsNull = true)
// |    |    |-- mates: array (nullable = true)
// |    |    |    |-- element: string (containsNull = true)
// |    |    |-- sname: string (nullable = true)
// |    |    |-- year: long (nullable = true)
  
  print(bakerSchema.select($"schools.mates".getItem(0).getItem(1)).show()) // mates containing an array. Extract further more to get the actual array values.
  
//  bakerSchema.printSchema()
  
//  val firstBaker = sqlContext.sql("select id, type, ppu, batter")
}