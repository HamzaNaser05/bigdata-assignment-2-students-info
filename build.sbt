name := "student-records-spark"

version := "0.1.0"

scalaVersion := "2.13.18"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark"   %% "spark-core"            % sparkVersion,
  "org.apache.spark"   %% "spark-sql"             % sparkVersion,
  "org.mongodb.spark"  %% "mongo-spark-connector" % "10.3.0"
)

fork := true

javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED"
)