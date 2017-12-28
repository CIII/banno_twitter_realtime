name := "banno_twitter_realtime"

version := "1.0"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" % Provided
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0" % Provided
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0" % Provided
libraryDependencies += "com.vdurmont" % "emoji-java" % "4.0.0"
libraryDependencies += "io.lemonlabs" %% "scala-uri" % "0.5.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}