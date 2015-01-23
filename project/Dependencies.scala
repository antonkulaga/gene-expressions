import sbt._

object Version {
  val spark        = "1.1.1"//"1.2.0"
  val hadoop       = "2.4.0"
  val scalaTest    = "2.2.1"
  val adam         = "0.15.0"  //"0.15.1-SNAPSHOT"
  val bdgFormats   = "0.5.1-SNAPSHOT"
  val guacamole    = "0.0.0"
  val scalaIO 	   = "0.4.3"
  val guava        = "14.0.1" //"18.0"
}

object Library {
  val sparkCore      = "org.apache.spark"  %% "spark-core" % Version.spark
  val sparkStreaming = "org.apache.spark"  %% "spark-streaming" % Version.spark
  val hadoopClient   = "org.apache.hadoop" %  "hadoop-client"   % Version.hadoop
  val scalaTest      = "org.scalatest"     %% "scalatest"       % Version.scalaTest
  val adamCore       = "org.bdgenomics.adam" % "adam-core" % Version.adam
  val bdFormats      = "org.bdgenomics.bdg-formats" % "bdg-formats" % Version.bdgFormats
  val guacamole      = "org.hammerlab.guacamole" % "guacamole" % Version.guacamole exclude("org.apache.spark","spark-core")
  val scalaIOFile =  "com.github.scala-incubator.io" %% "scala-io-file" % Version.scalaIO
  val guava =        "com.google.guava" % "guava" % Version.guava


}

object Dependencies {

  import Library._

  val sparkHadoop = Seq(
    sparkCore.exclude("org.apache.hadoop","hadoop-client"),
    sparkStreaming,
    hadoopClient,
    scalaTest % "test"
  )

/*  val other = Seq(
    guava,
    scalaIOFile
  )*/

  val genetics = Seq(
    adamCore,
    bdFormats
    //guacamole
  )

}
