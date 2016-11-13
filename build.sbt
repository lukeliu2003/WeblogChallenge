name := "WeblogChallenge"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-hive" % "1.6.0",
  "com.databricks" % "spark-csv_2.11" % "1.5.0",
  "com.opencsv" % "opencsv" % "3.8",
  "joda-time" % "joda-time" % "2.9.4",
  "org.scalactic" %% "scalactic" % "3.0.0",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test"
)