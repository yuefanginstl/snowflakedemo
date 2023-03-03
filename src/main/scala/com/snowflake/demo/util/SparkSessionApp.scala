package com.snowflake.demo.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

abstract class SparkSessionApp extends App{
  //abstract method to implement by child class
  def runApp: Unit

  lazy val logger = LoggerFactory.getLogger(this.getClass)
  val spark = SparkSession.builder.master("local[*]").appName("Test Spark Application").getOrCreate()
  val sparkContext = spark.sparkContext
  runApp
  spark.stop()

}
