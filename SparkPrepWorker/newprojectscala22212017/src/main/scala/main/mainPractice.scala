package main
import org.apache.spark.sql. functions._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.{SparkConf, SparkContext}

object mainPractice extends App {
  System.setProperty("hadoop.home.dir", "C:\\winutil")
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val babyNames = sc.textFile("names.txt")
  
  val sparkSession = SparkSession
  .builder()
  .appName("SQL App Basic")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()
  
  import sparkSession.implicits._
  
  val sqlContext = new org.apache.spark.sql.SQLContext(sc).read.format("CSV").option("header", "true").load("names.txt").toDF()
  sqlContext.printSchema()
  val resultQuery = sqlContext.groupBy("gender").count().show()
  
  print(resultQuery)
}