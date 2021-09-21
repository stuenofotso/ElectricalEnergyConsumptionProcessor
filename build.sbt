name := "ElectricalEnergyConsumptionProcessor"

version := "0.1"

scalaVersion := "2.11.8"

// grading libraries
libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies ++= Seq(
  "log4j" % "log4j" % "1.2.17",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.scala-lang" % "scala-compiler" % scalaVersion.value

)