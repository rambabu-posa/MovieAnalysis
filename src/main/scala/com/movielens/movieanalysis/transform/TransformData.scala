package com.movielens.movieanalysis.transform

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class TransformData(movies: DataFrame, ratings: DataFrame) {
  def moviesDataTransformation(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val ratingsAggs = ratings.groupBy("MovieID")
      .agg(max("Rating") as "maxRating"
        , min("Rating") as "minRating"
        , avg("Rating") as "avgRating")

    val moviesData = movies.as("mv")
      .join(ratingsAggs.as("rt"), $"mv.MovieID" === $"rt.MovieID", "left")
      .selectExpr("mv.MovieID as MovieID"
        , "mv.Title as Title"
        , "mv.Genres as Genres"
        , "rt.maxRating as maxRating"
        , "rt.minRating as minRating"
        , "rt.avgRating as avgRating")
    moviesData
  }

  def top3UserRatings(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val windowSpec = Window.partitionBy("UserID").orderBy(col("Rating").desc)
    val top3Ratings = ratings.withColumn("rank", row_number().over(windowSpec))
      .where("rank < 4")
    val top3RatingNames = top3Ratings.as("tp")
      .join(broadcast(movies).as("mv"), $"tp.MovieID" === $"mv.MovieID", "left")
      .selectExpr("tp.*", "mv.Title")
      .orderBy("UserID", "rank")
    top3RatingNames
  }
}
