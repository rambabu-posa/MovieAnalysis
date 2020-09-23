
scalaVersion     := "2.12.12"
version          := "0.1.0"
name             := "MovieAnalysis"

libraryDependencies ++= Seq("org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.13.0" % Runtime,
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "com.typesafe" % "config" % "1.2.1",
  "org.specs2" %% "specs2-junit" % "4.10.2" % Test)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
