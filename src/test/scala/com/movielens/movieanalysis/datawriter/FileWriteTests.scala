package com.movielens.movieanalysis.datawriter

import com.movielens.movieanalysis.helper.CreateSession
import org.specs2.mutable.Specification

class FileWriteTests extends Specification with CreateSession {
  "Using write method" should {
    import spark.implicits._
    val moviesMock = Seq(
      ("3903", "Urbania (2000)", "Drama"),
      ("3904", "Uninvited Guest, An (2000)", "Drama"),
      ("3905", "Specials, The (2000)", "Comedy"),
      ("3906", "Under Suspicion (2000)", "Crime")
    ).toDF("MovieID", "Title", "Genres")

    val moviesDataWriter = new FileWriter(moviesMock, "./src/test/tmp/moviesMock/")
    moviesDataWriter.write
    val writtenData = spark.read.parquet("./src/test/tmp/moviesMock/")
    val matchingDiff = moviesMock.except(writtenData)
    val matchingDiffRev = writtenData.except(moviesMock)
    "Written dataframe should match the count" in {
      writtenData.count() must_== 4
    }
    "Matching dataframe should return zero diff" in {
      matchingDiff.count() must_== 0
    }
    "Matching dataframe should return zero diff" in {
      matchingDiffRev.count() must_== 0
    }
  }
}