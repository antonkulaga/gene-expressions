//
// http://spark.apache.org/docs/latest/quick-start.html#a-standalone-app-in-scala
//
name := """gene-expressions"""

// 2.11 doesn't seem to work
scalaVersion := "2.10.4"

version := "0.0.1"

resolvers += "bintray/non" at "http://dl.bintray.com/non/maven"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Dependencies.sparkHadoop

libraryDependencies ++= Dependencies.other

libraryDependencies ++= Dependencies.genetics

releaseSettings

scalariformSettings
