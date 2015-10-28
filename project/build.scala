import sbt._
import Keys._

object DemoBuild extends Build {
  lazy val demoSettings = Defaults.defaultSettings ++ Seq(
    organization := "com.parser",
    version := "0.4",
    scalaVersion := "2.10.4",
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    javacOptions ++= Seq("-target", "1.6", "-source", "1.6"),
    crossPaths := false,
    libraryDependencies ++= Seq(
      "org.json4s" %% "json4s-native" % "3.2.11",
      "org.json4s" %% "json4s-jackson" % "3.2.11",
      "mysql" % "mysql-connector-java" % "5.1.21",
      "org.apache.hive" % "hive-exec" % "0.12.0",
      "org.apache.hadoop" % "hadoop-common" % "2.4.1" % "provided",
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.4.1" % "provided",
      "org.apache.hadoop" % "hadoop-core" % "0.20.2" % "provided"
    ),
    initialize ~= { _ => initSqltyped },
    resolvers ++= Seq(sonatypeNexusSnapshots, sonatypeNexusReleases)
  )

  lazy val demo = Project(
    id = "HiveParser",
    base = file("."),
    settings = demoSettings
  )
  
  val sonatypeNexusSnapshots = "Sonatype Nexus Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  val sonatypeNexusReleases = "Sonatype Nexus Releases" at "https://oss.sonatype.org/content/repositories/releases"
}
