import java.util

import org.bdgenomics.formats.avro.Feature
import org.denigma.genes.FixedGTFParser

import scala.io.Source
import framian.csv._
import framian._
import framian.csv.Csv
import spire.implicits._
import framian.csv.CsvFormat.CSV

import scala.reflect.io.File
def fromFile(path:String): String =  Source.fromFile(path).getLines().reduce(_+"\n"+_)
val data = "/home/antonkulaga/data/"
val gtf = data + "Drosophila_melanogaster.BDGP5.76_SHORT.gtf"
val stg = fromFile(gtf)
val p = new FixedGTFParser
val features: Seq[Feature] = p.parse(stg)
val gs = features.collect{
  case g /*if  g.getFeatureType=="gene"*/ => g}
gs
/*
//val xls = data + "genes_alex.csv"
val name = data+"genes_alex_short"
val fl = name+".csv"
val str = fromFile(fl)
val headers = ("test_id;" +
  "gene_id;" +
  "gene;" +
  "locus;" +
  "sample_1;" +
  "sample_2;" +
  "status;" +
  "value_1;" +
  "value_2;" +
  "log2(fold_change);" +
  "test_stat;" +
  "p_value;" +
  "q_value;" +
  "significant").split(";")
val format = CsvFormat(";").withHeader(true).withRowDelim("\n")
val genes = Csv.parseString(str,format).labeled.toFrame
val gn = Cols("gene").as[String]
val named = genes.map(gn,"gene")(a=>a)
val withNames = Csv.fromFrame(format)(named).labeled
val file: File = File(name+1+".csv")
file.deleteIfExists()
file.createFile()
val wr = file.writer(true)
withNames.header.foreach(h=>wr.write(h+";"))
withNames.data.foreach(r=>wr.write(r.render(format)))
wr.close()
*/
