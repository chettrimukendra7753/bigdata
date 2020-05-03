name := "Apache Log Parsing"

version := "0.0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.1"

// additional libraries
resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.holdenkarau"  %% "spark-testing-base" % "1.5.2_0.6.0" % "test"
  // "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test",
)
