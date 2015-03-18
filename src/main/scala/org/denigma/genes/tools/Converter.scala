package org.denigma.genes.tools

import org.denigma.genes.FixedGTFParser

import scala.io.Source
import org.bdgenomics.adam.rdd.ADAMContext._
/**
 * * converter to convert some data
 */
object Converter extends scala.App{
  val path = "/home/antonkulaga/data/"
  val samplesPath = "samples/"
  //val destination = "hdfs://localhost/user/antonkulaga/data/"
  val utrs = path + "utrs/"
  val destination = utrs + "output"



  def convertBam(filePath:String,destination: String) = {
    //val path = "/home/antonkulaga/data/"
    //val file = path + "test.sam"
    val destination = "hdfs://localhost/user/antonkulaga/data/"
    val output = destination+"9.adam"
    //val output = "hdfs://localhost/user/antonkulaga/data/test.adam"
    //val output = path
    val converter = new Bam2ADAM(filePath,output)
    converter.run()
  }




  //convertBam(samplesPath+"9.bam",destination)

}
