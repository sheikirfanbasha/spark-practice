name := "spark-practice"
version := "1.0"
scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "ml.combust.mleap" %% "mleap-spark" % "0.7.0",
  "ml.combust.mleap" %% "mleap-runtime" % "0.7.0"
)
