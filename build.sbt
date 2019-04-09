name := "untitled"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "org.apache.kudu" %% "kudu-spark2" % "1.7.1"
)