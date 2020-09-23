package com.movielens.movieanalysis

import com.movielens.movieanalysis.service.MoviesProcess
import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession


object MovieDriver extends Logging {

  def main(args: Array[String]): Unit = {
    implicit val config = ConfigFactory.parseResources("application.conf")

    val sparkBuilder = SparkSession
      .builder()
      .appName(config.getString("app.name"))

    implicit val spark = sparkBuilder.getOrCreate()

    try {
      logger.info("Process Started")
      MoviesProcess.process
      logger.info("Process finished")
    } catch {
      case ex: Exception => logger.error("Exception has occured" + ex.getStackTrace)
    }
    finally {
      spark.stop()
    }
  }
}
