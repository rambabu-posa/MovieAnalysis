package com.movielens.movieanalysis.helper

import org.apache.spark.sql.SparkSession

trait CreateSession {

  val sparkBuilder = SparkSession
    .builder()
    .appName("Movie Analysis Test")
    .master("local")
    .config("spark.driver.host", "localhost")

  implicit val spark: SparkSession = sparkBuilder.getOrCreate()

}
