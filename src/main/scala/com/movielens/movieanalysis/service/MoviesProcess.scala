package com.movielens.movieanalysis.service

import com.movielens.movieanalysis.datareader.FileReader
import com.movielens.movieanalysis.datawriter.FileWriter
import com.movielens.movieanalysis.transform.TransformData
import com.typesafe.config.Config
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession


object MoviesProcess extends Logging {

  def process(implicit spark: SparkSession, config: Config): Unit = {
    val moviesReader = new FileReader(config.getString("movies.delimiter"),
      config.getString("movies.path"), "MovieID,Title,Genres")
    val movies = moviesReader.read
      .selectExpr("cast(MovieID as bigInt) as MovieID",
        "Title",
        "Genres")
    val ratingsReader = new FileReader(config.getString("ratings.delimiter"),
      config.getString("ratings.path"), "UserID,MovieID,Rating,Timestamp")
    val ratings = ratingsReader.read
      .selectExpr("cast(UserID as bigInt) as UserID"
        , "cast(MovieID as bigInt) as MovieID"
        , "cast(Rating as Int) as Rating"
        , "Timestamp")
    val moviesTransform = new TransformData(movies, ratings)
    val moviesData = moviesTransform.moviesDataTransformation
    val top3RatingsNames = moviesTransform.top3UserRatings
    val moviesDataWriter = new FileWriter(moviesData, config.getString("moviesData.path"))
    moviesDataWriter.write
    val topRatingsNamesWriter = new FileWriter(top3RatingsNames, config.getString("top3Ratings.path"))
    topRatingsNamesWriter.write
  }

}
