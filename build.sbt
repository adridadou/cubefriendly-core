name := """cubefriendly-core"""

version := "0.1-SNAPSHOT"

organization := "org.cubefriendly"

scalaVersion := "2.11.6"

resolvers ++= Seq(
  "Local Maven Repository" at "file:///"+Path.userHome.absolutePath+"/.m2/repository" ,
  "Snapshot cubefriendly" at "http://cubefriendly-maven.s3.amazonaws.com/snapshot",
  "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/")

publishMavenStyle := true

publishTo := {
  val repoType = if (isSnapshot.value) "snapshot" else "release"
  Some(("Cubefriendly " + repoType) at "s3://cubefriendly-maven.s3.amazonaws.com/" + repoType)
}

timingFormat := {
  import java.text.DateFormat
  DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT)
}

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % "2.11.6",
  "org.scala-lang" % "scala-compiler" % "2.11.6",
  "org.scaldi" %% "scaldi" % "0.5.4",
  "org.cubefriendly" % "cube-engine" % "0.1-SNAPSHOT",
  "com.typesafe.akka" %% "akka-actor" % "2.3.9",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.9" % "test",
  "org.specs2" %% "specs2-core" % "2.4.15" % "test")
