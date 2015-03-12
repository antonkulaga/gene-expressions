package org.denigma.genes.tools

import org.bdgenomics.formats.avro.Feature
import org.denigma.genes.FixedGTFParser

object GeneRenamer extends scala.App{
  import scala.io.Source
  import framian.csv._
  import framian._
  import framian.csv.Csv
  import spire.implicits._
  import framian.csv.CsvFormat.CSV

  import scala.reflect.io.File
  def fromFile(path:String): String =  Source.fromFile(path).getLines().reduce(_+"\n"+_)
  val data = "/home/antonkulaga/data/"
  val name = "Drosophila_melanogaster.BDGP5.76.gtf" //"Drosophila_melanogaster.BDGP5.76_SHORT.gtf"
  val gtf = data + name
  val stg = fromFile(gtf)
  val p = new FixedGTFParser
  val features = p.parse(stg).view
  val file = File(data+"names.txt")
  file.deleteIfExists()
  file.createFile()
  val wr = file.writer(true)
  for{
    f<-features
    if f.getFeatureType=="gene"
  }  wr.write( f.attributes.get("gene_id")+";"+f.attributes.get("gene_name") )

  wr.close()
  println("FINISH")

}

