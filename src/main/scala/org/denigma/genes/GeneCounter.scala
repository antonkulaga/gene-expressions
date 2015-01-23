package org.denigma.genes

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{Exon, Gene, UTR}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.features.FeaturesContext._
import org.bdgenomics.adam.rdd.features.GeneFeatureRDD
import org.bdgenomics.adam.rdd.features.GeneFeatureRDD._
import org.bdgenomics.adam.rich.ReferenceMappingContext.FeatureReferenceMapping
import org.bdgenomics.formats.avro.Feature
import org.denigma.genes.parsers.FixedGTFParser

case class GeneCounter(@transient sc: SparkContext) {

  @transient
  lazy val ac = new ADAMContext(sc)

  /**
   * Just a test method that reads gtf and prints something to a file
   * @param from
   * @param to
   */
  def saveGenes(from: String, to: String) = {
    val features: RDD[Feature] = sc.adamGTFFeatureLoad(from)
    val genes = features.asGenes()
    val info= genes.map{
      case g=>
        s"-----------------\n"+
        s"GENE ${g.id} called ${g.names.mkString}\n"
        s"with transcripts: "+g.transcripts.foldLeft(""){
          case (acc,el)=> acc+s"id = ${el.id} with UTRS ${el.utrs.mkString}}"+"\n"
        }
    }
    info.coalesce(1,shuffle = true).saveAsTextFile(to)
    println("FEATURES SAVED SUCCESSFULLY")
  }

  /**
   * converts features to ADAM format
   * @param from
   * @param to
   */
  def convertFeatures(from:String,to:String) = {
    // get file extension
    // regex: anything . (extension) EOL
    val extensionPattern = """.*[.]([^.]*)$""".r
    val extensionPattern(extension) = from
    val features: RDD[Feature] = extension.toLowerCase match {
      case "gff"        => sc.adamGTFFeatureLoad(from)
      case "gtf"        => sc.adamGTFFeatureLoad(from)
      case "bed"        => sc.adamBEDFeatureLoad(from)
      case "narrowPeak" => sc.adamNarrowPeakFeatureLoad(from)
    }
    features.coalesce(1,true).adamParquetSave(to)
  }

  def convertBam(from:String,to:String) = {

  }

  def parseGTF(filePath:String): RDD[Feature] = {
    sc.textFile(filePath).flatMap(new FixedGTFParser().parse)
  }

  def openAdam(from:String) = {
    //sc.adamLoad()
  }


  protected def featuresByKey(features:RDD[Feature]): RDD[(String, Feature)] = {  features.keyBy(_.getFeatureType).cache()  }

  protected def utrsByTranscript(typePartitioned:RDD[(String, Feature)] ): RDD[(String, Iterable[UTR])] = {
    typePartitioned.filter(_._1 == "UTR").flatMap {
      case ("UTR", ftr: Feature) =>
        val ids: Seq[String] = ftr.getParentIds
        ids.map(transcriptId => (transcriptId,
          UTR(transcriptId, GeneFeatureRDD.strand(ftr.getStrand), FeatureReferenceMapping.getReferenceRegion(ftr))))
    }.groupByKey()
  }

  protected def exonsByTranscript(typePartitioned:RDD[(String, Feature)] ): RDD[(String, Iterable[Exon])] = {
    typePartitioned.filter(_._1 == "exon").flatMap {
      // There really only should be _one_ parent listed in this flatMap, but since
      // getParentIds is modeled as returning a List[], we'll write it this way.
      case ("exon", ftr: Feature) =>
        val ids: Seq[String] = ftr.getParentIds
        ids.map(transcriptId => (transcriptId,
          Exon(ftr.getFeatureId, transcriptId, GeneFeatureRDD.strand(ftr.getStrand), FeatureReferenceMapping.getReferenceRegion(ftr))))
    }.groupByKey()
  }


  protected def regionInfo(utr:UTR)=  s"from ${utr.region.start} to ${utr.region.end} of ${utr.region.length()}"


  def compareTranscripts(from:String*) = {
    val samples: Seq[(String, RDD[Feature])] = from.map(f=>f->this.parseGTF(f))
    val typedSamples: Seq[(String, RDD[(String, Feature)])] = samples.map{case(s,features)=>s->features.keyBy(_.getFeatureType).cache() }

  }

  def compareExons(from:String*)(to:String) = {
    val samples: Seq[(String, RDD[Feature])] = from.map(f=>f->this.parseGTF(f))
    val typedSamples: Seq[(String, RDD[(String, Feature)])] = samples.map{case(s,features)=>s->features.keyBy(_.getFeatureType).cache() }
    //val exonsSamples = typedSamples.map{ case(s,fs)=>s->exonsByTranscript(fs) }
    val exonsSamples = typedSamples.map{ case(s,fs)=>exonsByTranscript(fs) }
    sc.union(exonsSamples).saveAsTextFile(to)
  }

  /**
   * Compares UTRs
   * @param from
   * @param to
   * @return
   */
  def compareUTRS(from:String*)(to:String) = {
   val samples: Seq[(String, RDD[Feature])] = from.map(f=>f->this.parseGTF(f))
   val typedSamples: Seq[(String, RDD[(String, Feature)])] = samples.map{case(s,features)=>s->features.keyBy(_.getFeatureType).cache() }

   val utrSamples: Seq[(String, RDD[(String, Iterable[UTR])])] = typedSamples.map{ case(s,fs)=>s->utrsByTranscript(fs) }
   val info: RDD[(String, String)] = sc.union( utrSamples.map{
     case (sample,uters)=>
       uters.map{
         case (tr,uts)=>
           val info = sample+": "+uts.foldLeft("")((acc,el)=>acc+regionInfo(el))+"\n"
           tr-> info
          }
      } )
    val comparison: RDD[String] = info.groupByKey().map{
      case (key,strs)=>s"----------------------\nTRANSCRIPT $key"+strs.reduce(_+_)
   }
    comparison.coalesce(1,true).saveAsTextFile(to)*/
    println("GTF FILES: \n"+from.reduce((a,b)=>a+"\n"+b)+"\nwere processed")

  }

}
