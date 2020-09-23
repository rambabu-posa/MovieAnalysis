MOVIE ANALYSIS
An application to analyse the movie ratings from the data provided from movielens.

Getting Started
Project can be assembled using "sbt assembly", this will run the unit tests automatically with out any help 
build the required jar that is used as part of spark-submit job.

Config: Please provide the input configurations in the file main/resources/application.conf


spark-submit command for running the job:

spark-submit --class com.movielens.movieanalysis.MovieDriver  --master yarn  
--conf spark.dynamicAllocation.enabled=true --conf spark.driver.memory=10g --conf spark.executor.cores=4 
--conf spark.executor.memory=8g MovieAnalysis-assembly-0.1.0.jar

The above spark-submit configurations are for processing large amount of data, not meant for the files given by
movielens.

