ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.0"

// https://mvnrepository.com/artifact/commons-io/commons-io
libraryDependencies += "commons-io" % "commons-io" % "2.6"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.3"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.6" % "provided"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.4.1" % "provided"


//mysql connector i need to add below Jars
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.28"
// Add the path to the mysql-connector-java.jar file
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.28" from "file:///path/to/mysql-connector-java.jar"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.23"

lazy val root = (project in file("."))
  .settings(
    name := "churnprojenkins"
  )
