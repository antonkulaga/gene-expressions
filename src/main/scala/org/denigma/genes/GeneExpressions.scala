package org.denigma.genes

import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, SparkConf, SparkContext }
import org.bdgenomics.adam.models.Transcript
import org.bdgenomics.adam.rdd.{BroadcastRegionJoin, ADAMContext}
import org.bdgenomics.formats.avro.Feature

/**
 * Just  a test app to learn ADAM
 */
object GeneExpressions extends App {

  val home = System.getenv().getOrDefault("HOME","/home/antonkulaga")
  val sparkHome = System.getenv().getOrDefault("SPARK_HOME",s"$home/Soft/spark")
  val master = "spark://antonkulaga:7077"//"spark://"+System.getenv().getOrDefault("MASTER_SPARK","antonkulaga:1234")

  val conf: SparkConf = new SparkConf()
//    .setMaster(master) //.setMaster("yarn-client")
    //.setJars(Seq("target/scala-2.10/gene-expressions_2.10-0.0.1.jar"))
    //.setJars(Seq("target/scala-2.10/gene-expressions.jar"))
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
    .set("spark.kryoserializer.buffer.mb", "4")
    .set("spark.kryo.referenceTracking", "true")
    .set("spark.executor.memory", "6g")
    .setSparkHome(sparkHome)
    .setAppName("GeneExpressions")
  implicit val sc = new SparkContext(conf)


  println("CONFIGURATION IS:")

  println(sc.getConf.toDebugString)

  val path = "/home/antonkulaga/data/"
  val samplesPath = path+ "samples/"
  //val destination = "hdfs://localhost/user/antonkulaga/data/"
  val utrs = path + "utrs/"
  val destination = utrs + "output/"
  val reference = path + "Drosophila_melanogaster.BDGP5.76.gtf"

  val gs = new GeneExtractor(sc)
  println("received ids")

  val ids= gs.loadGenesId(utrs+"ids.txt")
  //println(s"ids = ${ids.mkString(" | ")}")
  //println("starting convertion of 3")
  gs.convertGTF(samplesPath+"3.gtf",destination+"features_3.adam")
  //println("starting convertion of 4")
  gs.convertGTF(samplesPath+"4.gtf",destination+"features_4.adam")
  //println("starting convertion of 9")
  gs.convertGTF(samplesPath+"9.gtf",destination+"features_9.adam")

  val geneFeaturs = gs.getFeatures(destination+"genes.adam")
  gs.cutGenes(destination+"features_3.adam",destination+"features_small_3.txt",ids)
  gs.cutGenes(destination+"features_4.adam",destination+"features_small_4.txt",ids)
  gs.cutGenes(destination+"features_9.adam",destination+"features_small_9.txt",ids)




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
