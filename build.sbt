name := "EODPosCalculator"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-repl
libraryDependencies += "org.apache.spark" %% "spark-repl" % "2.3.1" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-catalyst
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % "2.3.1" % Test

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1" % "provided"




