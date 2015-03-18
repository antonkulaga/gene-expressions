package org.denigma.genes


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.{BroadcastRegionJoin, ADAMRDDFunctions, ADAMContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.models._
import scala.io.Source


case class GeneExtractor(@transient sc:SparkContext)  extends ExpressionsBase{

  @transient lazy val ac = new ADAMContext(sc)

  /*def regionsFromGenes(genes:RDD[Gene]): Array[ReferenceRegion] = genes.flatMap(g=>g.regions).collect()
*/
/*  def getGenes(rdd:RDD[AlignmentRecord], regions: Array[ReferenceRegion]) = rdd.filter{  case rec =>
      ReferenceRegion(rec) match {
        case None => false
        case Some(reg)=> regions.exists(reg.overlaps)
      }
    }*/

  def loadGenesId(filePath:String) = sc.textFile(filePath).collect().toSet

  def getFeatures(filePath:String) = sc.textFile(filePath).flatMap(new FixedGTFParser().parse)

  /**
   * Saves only gene features
   * @param filePath
   * @param destination
   */
  def saveGenesOnly(filePath:String,destination:String) = {
    val features: RDD[Feature] =  this.getFeatures(filePath)
    features.filter(f=>f.getFeatureType=="gene").adamParquetSave(destination)
  }

  def convertGTF(filePath:String,destination: String) = {
    val features: RDD[Feature] = this.getFeatures(filePath)
    features.adamParquetSave(destination)
  }

  def convertGTF(filePath:String,destination: String, geneFeatures:RDD[Feature]) = {
    val features: RDD[Feature] =  this.getFeatures(filePath) ++ geneFeatures
    features.adamParquetSave(destination)
  }

  /*def cutGenes(filePath:String,destination:String,ids:Set[String]) = {
    val genes = ac.loadGenes(filePath)
    val smaller: RDD[Gene] = genes.filter(g=>ids.contains(g.id))
    smaller.saveAsObjectFile(destination)
  }
  */
  def loadFeatures(filePath:String) = ac.loadFeatures(filePath)

  def cutGenes(filePath:String,destination:String,ids:Set[String]) = loadFeatures(filePath)
      .filter{   case f=> ids.contains(f.getAttributes.get("gene_id"))     }
      .adamParquetSave(destination)

  def join(features:RDD[Feature],reads:RDD[AlignmentRecord]): RDD[(Feature, AlignmentRecord)] = {
    import scala.reflect._
    BroadcastRegionJoin.partitionAndJoin(sc,features,reads)(
      org.bdgenomics.adam.rich.ReferenceMappingContext.FeatureReferenceMapping,
      org.bdgenomics.adam.rich.ReferenceMappingContext.AlignmentRecordReferenceMapping,classTag[Feature],classTag[AlignmentRecord])
  }

  def counts(features:RDD[Feature],reads:RDD[AlignmentRecord]): RDD[(String, Int)] = {
    join(features,reads).groupBy{  case (f,a)=>f.getFeatureId  }.map{
      case (f,i)=>f->i.size
    }
  }

  def countsByGeneIds(features:RDD[Feature],reads:RDD[AlignmentRecord]): RDD[(String, Int)] = {
    join(features,reads).groupBy{  case (f,a)=>f.getAttributes.get("gene_id")  }.map{
      case (f,i)=>f->i.size
    }
  }

  def countsByGeneIdsAndLen(features:RDD[Feature],reads:RDD[AlignmentRecord]): RDD[((String, Long), Int)] = {
    join(features,reads).groupBy{  case (f,a)=>(f.getAttributes.get("gene_id"),Math.abs(f.getEnd-f.getStart))  }.map{
      case (f,i)=>f->i.size
    }
  }

  def saveCounts(features:RDD[Feature],reads:RDD[AlignmentRecord],filePath:String): RDD[(String, Int)] = {
    val cs = counts(features,reads)
    cs.saveAsTextFile(filePath)
    cs
  }

  def saveCountsByGeneIdsAndLen(features:RDD[Feature],reads:RDD[AlignmentRecord],filePath:String): RDD[((String, Long), Int)] = {
    val cs = countsByGeneIdsAndLen(features,reads)
    cs.saveAsTextFile(filePath)
    cs
  }



  //def getGenes(filePath:String)=  sc.objectFile[Gene](filePath)







}
