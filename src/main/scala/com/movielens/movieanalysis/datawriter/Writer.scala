package com.movielens.movieanalysis.datawriter

import org.apache.spark.sql.SparkSession

trait Writer {
  def write(implicit spark: SparkSession): Unit
}
