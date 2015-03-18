package org.denigma.genes

import org.apache.avro.Schema
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{Transcript, CDS, Exon, UTR}
import org.bdgenomics.adam.predicates.HighQualityReadPredicate
import org.bdgenomics.adam.projections.{AlignmentRecordField, Projection}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.comparisons.ComparisonTraversalEngine
import org.bdgenomics.adam.rich.ReferenceMappingContext.FeatureReferenceMapping
import org.bdgenomics.formats.avro.{AlignmentRecord, Feature, Strand}
import ADAMContext._


case class GeneCounter(@transient sc: SparkContext) extends ExpressionsBase{



  @transient
  lazy val ac = new ADAMContext(sc)

  
  def loadGTFFeatures(filePath:String) = {
    sc.textFile(filePath).flatMap(new FixedGTFParser().parse)
  }
  
  def loadFeatures(input:String,name:String):RDD[(String,Feature)]= {
    val filePath = input + name
    val features: RDD[Feature] = this.loadGTFFeatures(filePath)
    featuresByKey(features).cache()
  }


  def loadGenes(input:String,name:String)= {
    val byKey = this.loadFeatures(input,name)
    val exons = this.exonsByTranscript(byKey)
    val utrs = this.utrsByTranscript(byKey)
    exons.take(100).foreach{
      e=>println(s"EXON = "+e._2+s" of TRANSCRIPT ${e._1} \n")
    }
    val cds: RDD[(String, Iterable[CDS])] = this.cdsByTranscript(byKey)
    transcriptsByGenes(byKey,exons,utrs,cds)
  }

  
  def loadGeneMap(names:String*)(implicit path:String): Map[String, RDD[AlignmentRecord]] = {
    val files = names.map(n=>path+n)
    val pred = new HighQualityReadPredicate
    files.map(f=>f->ac.loadAlignments(f,predicate = Some(classOf[HighQualityReadPredicate]),projection =Some(projection))).toMap
  }


  def featuresByKey(features:RDD[Feature]): RDD[(String, Feature)] = {  features.keyBy(_.getFeatureType).cache()  }

  
  def cdsByTranscript(typePartitioned:RDD[(String, Feature)] ): RDD[(String, Iterable[CDS])] = {

      typePartitioned.filter(_._1 == "CDS").flatMap {
        case ("CDS", ftr: Feature) =>
          val ids: Seq[String] = ftr.getParentIds.map(_.toString)
          ids.map(transcriptId => (transcriptId,
            CDS(transcriptId, strand(ftr.getStrand), FeatureReferenceMapping.getReferenceRegion(ftr))))
      }.groupByKey()
  }
  
  def utrsByTranscript(typePartitioned:RDD[(String, Feature)]): RDD[(String, Iterable[UTR])] = {
      typePartitioned.filter(_._1 == "UTR").flatMap {
        case ("UTR", ftr: Feature) =>
          val ids: Seq[String] = ftr.getParentIds.map(_.toString)
          ids.map(transcriptId => (transcriptId,
            UTR(transcriptId, strand(ftr.getStrand), FeatureReferenceMapping.getReferenceRegion(ftr))))
      }.groupByKey()
    
  }

  def exonsByTranscript(typePartitioned:RDD[(String, Feature)] ): RDD[(String, Iterable[Exon])] = {
    typePartitioned.filter(_._1 == "exon").flatMap {
      case ("exon", ftr: Feature) =>
        val ids: Seq[String] = ftr.getParentIds
        ids.map(transcriptId => (transcriptId,
          Exon(ftr.getFeatureId, transcriptId, strand(ftr.getStrand), FeatureReferenceMapping.getReferenceRegion(ftr))))
    }.groupByKey()
  }

  def transcriptsByGenes(typePartitioned:RDD[(String,Feature)],
                         exonsByTranscript:RDD[(String, Iterable[Exon])],
                         utrsByTranscript:RDD[(String, Iterable[UTR])],
                           cdsByTranscript: RDD[(String, Iterable[CDS])] ) = {
    typePartitioned.filter(_._1 == "transcript").map {
      case ("transcript", ftr: Feature) => (ftr.getFeatureId.toString, ftr)
    }.join(exonsByTranscript)
      .leftOuterJoin(utrsByTranscript)
      .leftOuterJoin(cdsByTranscript)
      .flatMap {
      // There really only should be _one_ parent listed in this flatMap, but since
      // getParentIds is modeled as returning a List[], we'll write it this way.
      case (transcriptId: String, (((tgtf: Feature, exons: Iterable[Exon]),
      utrs: Option[Iterable[UTR]]),
      cds: Option[Iterable[CDS]])) =>
        val geneIds: Seq[String] = tgtf.getParentIds.map(_.toString) // should be length 1
        geneIds.map(f = geneId => (geneId,
          Transcript(transcriptId, Seq(transcriptId), geneId,
            strand(tgtf.getStrand),
            exons, cds.getOrElse(Seq()), utrs.getOrElse(Seq()))))
    }.groupByKey()
  }

  /*  
  
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
    }*/

  protected def regionInfo(utr:UTR)=  s"from ${utr.region.start} to ${utr.region.end} of ${utr.region.length()}"


/*
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
    comparison.coalesce(1,true).saveAsTextFile(to)
    println("GTF FILES: \n"+from.reduce((a,b)=>a+"\n"+b)+"\nwere processed")

  }
*/
}
