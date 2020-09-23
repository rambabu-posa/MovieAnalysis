package com.movielens.movieanalysis.datareader

import org.apache.spark.sql.{DataFrame, SparkSession}

class FileReader(delimiter: String, path: String, columnNames: String) extends Reader {
  def read(implicit spark: SparkSession): DataFrame = {
    spark.read.option("delimiter", delimiter).csv(path).toDF(columnNames.split(","): _*)
  }
}