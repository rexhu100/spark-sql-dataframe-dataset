name := "spark-sql-dataframe-dataset"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.1.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.1" % "provided"
