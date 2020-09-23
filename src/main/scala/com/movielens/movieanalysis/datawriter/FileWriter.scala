package com.movielens.movieanalysis.datawriter

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


class FileWriter(df: DataFrame, path: String) extends Writer {
  def write(implicit spark: SparkSession): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(path)
  }
}
