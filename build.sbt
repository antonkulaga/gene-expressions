name := """gene-expressions"""

parallelExecution in Test := false


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

com.github.retronym.SbtOneJar.oneJarSettings

mainClass in oneJar := Some("org.denigma.genes.GeneExpressions")