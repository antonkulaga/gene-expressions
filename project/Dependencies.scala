import sbt._

object Version {
  val spark        = "1.2.1"
  val hadoop       = "2.4.0"//"2.5.1"
  val scalaTest    = "2.2.1"
  val adam         = "0.16.0"  //"0.15.1-SNAPSHOT"
  //val bdgFormats   = "0.5.1-SNAPSHOT"
  val bdgFormats   = "0.5.0"
  val guacamole    = "0.0.0"
  val scalaIO 	   = "0.4.3"
  val guava        = "14.0.1" //"18.0"
  val productCollections = "1.4.1"
  val framian =  "0.3.3"
}

object Library {
  val  productCollections        = "com.github.marklister" %% "product-collections" % Version.productCollections
  val framian =     "com.pellucid" %% "framian" % Version.framian
  val sparkCore      = "org.apache.spark"  %% "spark-core" % Version.spark
  val sparkStreaming = "org.apache.spark"  %% "spark-streaming" % Version.spark
  val hadoopClient   = "org.apache.hadoop" %  "hadoop-client"   % Version.hadoop
  val scalaTest      = "org.scalatest"     %% "scalatest"       % Version.scalaTest
  val adamCore       = "org.bdgenomics.adam" % "adam-core" % Version.adam
  val bdFormats      = "org.bdgenomics.bdg-formats" % "bdg-formats" % Version.bdgFormats
  val guacamole      = "org.hammerlab.guacamole" % "guacamole"
  val scalaIOFile =  "com.github.scala-incubator.io" %% "scala-io-file" % Version.scalaIO
  val guava =        "com.google.guava" % "guava" % Version.guava


}

object Dependencies {

  import Library._

  val common = Seq(
    framian,
    productCollections)

  val sparkHadoop = Seq(
    sparkCore,
    //sparkStreaming,
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
