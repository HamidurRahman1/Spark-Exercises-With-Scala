name := "Spark-Exercises-With-Scala"

version := "0.1"

scalaVersion := "2.12.14"
val sparkVersion = "3.1.2"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies ++= sparkDependencies

