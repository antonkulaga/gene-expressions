package org.denigma.genes.tools

/**
 * * converter to convert some data
 */
object Converter extends scala.App{
  
  //val path = "/home/antonkulaga/data/"
  //val file = path + "test.sam"
  val path = "/home/antonkulaga/data/samples/"
  val file = path+"9.bam"
  val destination = "hdfs://localhost/user/antonkulaga/data/"
  val output = destination+"9.adam"
  //val output = "hdfs://localhost/user/antonkulaga/data/test.adam"
  //val output = path
  val converter = new Bam2ADAM(file,output)
  converter.run()
  
}
