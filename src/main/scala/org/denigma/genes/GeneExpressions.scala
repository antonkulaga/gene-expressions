package org.denigma.genes

import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, SparkConf, SparkContext }

/**
 * Just  a test app to learn ADAM
 */
object GeneExpressions extends App {

  val home = System.getenv().getOrDefault("HOME","/home/antonkulaga")
  val sparkHome = System.getenv().getOrDefault("SPARK_HOME",s"$home/Soft/spark")
  val master = System.getenv().getOrDefault("SPARK_MASTER","spark://antonkulaga:7077")

  val conf: SparkConf = new SparkConf()
    .setMaster(master) //.setMaster("yarn-client")
    //.setJars(Seq("target/scala-2.10/gene-expressions.jar"))
    .setSparkHome(sparkHome)
    .setAppName("GeneExpressions")
  implicit val sc = new SparkContext(conf)

  val prefix = s"${home}/data/" //path to gr file
  val adam = s"$prefix/adam"
  val output = s"$prefix/output"

  val gc = GeneCounter(sc)
  val gtf = "Drosophila_melanogaster.BDGP5.76.gtf" //gtf file to read
  //gc.countReads(s"$adam/3",s"$output/adam.txt")
  gc.printFeatures(prefix + gtf, s"$output/test.txt") //writes gtf to a fly.txt file
  println("SPARK JOB SUCCESSFULLY FINISHED")
  sc.stop
}
