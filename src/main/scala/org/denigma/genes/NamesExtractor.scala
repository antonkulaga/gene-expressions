package org.denigma.genes

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext

import scala.collection.Map
import scala.io.Source
import scala.reflect.io.File
import org.apache.spark.SparkContext._ //some useful implicits

case class NamesExtractor(@transient sc: SparkContext)  {
  @transient
  lazy val ac = new ADAMContext(sc)


  def fromFile(path:String): String =  Source.fromFile(path).getLines().reduce(_+"\n"+_)


  /**
   * Maps genes name
   * @param mp
   * @param gene
   * @return
   */
  def mapGene(mp:Map[String,String],gene:String) = {
    if(gene.contains(','))
      gene.split(',').foldLeft("")((acc,el)=> mp.getOrElse(el, el) )
    else  mp.getOrElse(gene, gene)
  }

  /**
   * Saves a table GeneId -> GeneSymbol
   * @param path
   * @param name
   */
  def saveNames(path:String =  "/home/antonkulaga/data/",name:String = "Drosophila_melanogaster.BDGP5.76.gtf") = {

    val gtf = path + name
    //val p = new FixedGTFParser
    //val features = p.parse(stg).view
    val fs = ac.loadFeatures(gtf)
    val names = fs.collect{case f if f.getFeatureType=="gene"=> f.attributes.get("gene_id")->f.attributes.get("gene_name") }
    names.saveAsObjectFile(path+"names.txt")
    println("geneid->genename table was SAVED!")

  }

  /**
   * Uses file of geneid->genename created by saveNames form GTF to write updated CSV
   * @param path
   * @param name
   * @param sourceName
   * @return
   */
  def transformNames(path:String =  "/home/antonkulaga/data/",name:String = "names.txt", sourceName:String = "genes_alex") =
  {
    import scala.io.Source
    import framian.csv._
    import framian._
    import framian.csv.Csv
    import spire.implicits._
    import framian.csv.CsvFormat.CSV

    val names: Map[String, String] = sc.objectFile[(String,String)](path+name).collectAsMap()
    val fl = path+sourceName+".csv"
    val str = fromFile(fl)
    val format = CsvFormat(";").withRowDelim("\n").withHeader(true)
    val sig = Cols("significant").as[String]
    val raw = Csv.parseString(str,format).labeled
    val genes = raw.toFrame.filter(sig)(s=>s=="yes")
    val gn: Cols[String, String] = Cols("gene").as[String]
    val named: Frame[Int, String] = genes.map(gn,"gene")(g=> this.mapGene(names,g))
    val withNames = Csv.fromFrame(format)(named)
    val file: File = File(path+sourceName+"_1"+".csv")
    file.deleteIfExists()
    file.createFile()
    val wr = file.writer(true)
    wr.write(withNames.toString())
    wr.close()
    println("writing gene-ids finished!!!!!!!!!")
  }

}
