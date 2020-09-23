package com.movielens.movieanalysis.transform

import com.movielens.movieanalysis.helper.CreateSession
import org.apache.spark.sql.DataFrame
import org.specs2.mutable.Specification

class TranformDataTests extends Specification with CreateSession {
  "Using tranform method" should {
    import spark.implicits._
    val movies = Seq(
      (1, "abc", "cef"),
      (2, "bcd", "cef"),
      (3, "def", "fgh"))
      .toDF("MovieID", "Title", "Genres")
    val ratings = Seq(
      (100, 1, 5, "964828575"),
      (101, 1, 4, "964828575"),
      (102, 1, 3, "964828575"),
      (103, 2, 4, "964828575"),
      (104, 2, 3, "964828575"),
      (105, 2, 2, "964828575"),
      (100, 3, 3, "964828575"),
      (101, 3, 2, "964828575"),
      (102, 3, 1, "964828575"))
      .toDF("UserID", "MovieID", "Rating", "Timestamp")
    val moviesTransform = new TransformData(movies, ratings)
    val moviesData = moviesTransform.moviesDataTransformation
    val top3RatingsNames = moviesTransform.top3UserRatings
    val expectedMovieData = Seq(
      (1, "abc", "cef", 5, 3, 4.0),
      (2, "bcd", "cef", 4, 2, 3.0),
      (3, "def", "fgh", 3, 1, 2.0)).toDF("MovieID", "Title", "Genres", "maxRating", "minRating", "avgRating")
    val matchingDiff = moviesData.except(expectedMovieData)
    val matchingDiffRev = expectedMovieData.except(moviesData)
    val expectedTop3RatingNames = Seq(
      (100, 1, 5, "964828575", 1, "abc"),
      (100, 3, 3, "964828575", 2, "def"),
      (101, 1, 4, "964828575", 1, "abc"),
      (101, 3, 2, "964828575", 2, "def"),
      (102, 1, 3, "964828575", 1, "abc"),
      (102, 3, 1, "964828575", 2, "def"),
      (103, 2, 4, "964828575", 1, "bcd"),
      (104, 2, 3, "964828575", 1, "bcd"),
      (105, 2, 2, "964828575", 1, "bcd")).toDF("UserID", "MovieID", "Rating", "Timestamp", "rank", "Title")
    val top3MatchingDiff = top3RatingsNames.except(expectedTop3RatingNames)
    val top3MatchingDiffRev = expectedTop3RatingNames.except(top3RatingsNames)

    "Return a dataframe" in {
      moviesData must haveClass[DataFrame]
    }
    "Return a dataframe" in {
      top3RatingsNames must haveClass[DataFrame]
    }

    "transformed dataframe should match the count" in {
      moviesData.count() must_== 3
    }

    "top3RatingsNames dataframe should match the count" in {
      top3RatingsNames.count() must_== 9
    }

    "Matching dataframe should return zero diff" in {
      matchingDiff.count() must_== 0
    }

    "Matching dataframes in reverse order should return zero diff" in {
      matchingDiffRev.count() must_== 0
    }
    "Matching dataframe should return zero diff" in {
      top3MatchingDiff.count() must_== 0
    }

    "Matching dataframes in reverse order should return zero diff" in {
      top3MatchingDiffRev.count() must_== 0
    }

  }
}
