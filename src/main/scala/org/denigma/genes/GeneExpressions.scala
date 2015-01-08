package org.denigma.genes

import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, SparkConf, SparkContext }

/**
 * Just  a test app to learn ADAM
 */
object GeneExpressions extends App {

  val sparkHome = "/home/antonkulaga/Soft/spark"

  val master = "spark://antonkulaga:7077"

  val conf: SparkConf = new SparkConf()
    .setMaster(master) //.setMaster("yarn-client")
    .setJars(Seq("target/scala-2.10/gene-expressions_2.10-0.0.1.jar"))
    .setSparkHome(sparkHome)
    .setAppName("GeneExpressions")
  implicit val sc = new SparkContext(conf)

  val prefix = "/home/antonkulaga/data/" //path to gr file


  val gc = GeneCounter(sc)
  val fileName = "Drosophila_melanogaster.BDGP5.76.gtf" //gtf file to read
  gc.printFeatures(prefix + fileName, prefix + "output/" + "fly.txt") //writes gtf to a fly.txt file
  sc.stop
}
