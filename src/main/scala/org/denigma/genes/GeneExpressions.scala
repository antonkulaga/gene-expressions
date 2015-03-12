package org.denigma.genes

import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, SparkConf, SparkContext }
import org.bdgenomics.adam.models.Transcript
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.Feature

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
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
    .set("spark.kryoserializer.buffer.mb", "4")
    .set("spark.kryo.referenceTracking", "true")
    .set("spark.executor.memory", "6g")
    .setSparkHome(sparkHome)
    .setAppName("GeneExpressions")
  implicit val sc = new SparkContext(conf)
  val ns = new NamesExtractor(sc)
  //ns.saveNames()
  ns.transformNames()
  sc.stop()
  
/*
  val localPrefix = s"$home/data/" //path to gr file
  val samples = localPrefix + "samples/"
  val hdfs = "hdfs://localhost/user/antonkulaga/data/"
  val gc = GeneCounter(sc)
  val gtfName = "merged.gtf"
  val features  = gc.loadFeatures(samples,gtfName)
  features.collect{  case ("transcript",feature)=>feature}
  
/*  val genes = gc.loadGenes(samples,gtfName)
  val gs: Array[(String, Iterable[Transcript])] = genes.takeSample(false,100)
  for{
    (id,ts) <- gs
    t <-ts
  } {
    println(s"GENE $id with transcript $t \n")
    
  }*/
  println("GENE EXPRESSIONS SUCCESSFULY FINISHED")
  //gc.loadGenes("3.adam","4.adam","9.adam")(hdfs)*/

  

  
  sc.stop()
}
