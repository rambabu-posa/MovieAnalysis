package com.movielens.movieanalysis.datareader

import com.movielens.movieanalysis.helper.CreateSession
import org.apache.spark.sql.DataFrame
import org.specs2.mutable.Specification

class FileReaderTests extends Specification with CreateSession {
  "Using read method" should {
    import spark.implicits._
    val moviesReader = new FileReader("::", "./src/test/resources/movies.dat", "MovieID,Title,Genres")
    val moviesRead = moviesReader.read
    val moviesMock = Seq(
      ("3903", "Urbania (2000)", "Drama"),
      ("3904", "Uninvited Guest, An (2000)", "Drama"),
      ("3905", "Specials, The (2000)", "Comedy"),
      ("3906", "Under Suspicion (2000)", "Crime")
    ).toDF("MovieID", "Title", "Genres")
    val matchingDiff = moviesMock.except(moviesRead)
    val matchingDiffRev = moviesRead.except(moviesMock)

    "Return a dataframe" in {
      moviesRead must haveClass[DataFrame]
    }

    "read dataframe should match the count" in {
      moviesRead.count() must_== 4
    }

    "Matching dataframe should return zero diff" in {
      matchingDiff.count() must_== 0
    }

    "Matching dataframes in reverse order should return zero diff" in {
      matchingDiffRev.count() must_== 0
    }
  }
}
