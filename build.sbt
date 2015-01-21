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

mainClass in assembly := Some("org.denigma.genes.GeneExpressions")

assemblyJarName in assembly := "gene-expressions.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) { (old) =>
{
  case m if m.toLowerCase.matches("meta-inf/.*\\.sf$") => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case x =>  MergeStrategy.last
}  }

assemblyExcludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp filter {_.data.getName.contains("hadoop")}
}