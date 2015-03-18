
object Cells {
  import scala.io.Source
  import org.apache.spark._
  import org.bdgenomics.adam.rdd.ADAMContext
  import org.bdgenomics.adam.rdd.ADAMContext._
  import org.bdgenomics.adam.projections._
  import org.bdgenomics.adam.rdd.features._
  import org.bdgenomics.formats.avro._
  import org.bdgenomics.adam.models._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.SparkContext
  import org.bdgenomics.adam.rdd._
  import org.bdgenomics.adam.rdd.features.{GTFParser, FeatureParser}
  
  
  @transient def sc = sparkContext
  @transient lazy val ac = new ADAMContext(sc)
  
  val path = "/home/antonkulaga/data/"
  val samplesPath = path+ "samples/"
  val utrs = path + "utrs/"
  val destination = utrs + "output/"
  val reference = path + "Drosophila_melanogaster.BDGP5.76.gtf"

  /* ... new cell ... */

  def loadFeatures(filePath:String) = ac.loadFeatures(filePath).map(f =>  
      if(f.getFeatureType=="exon" && f.getFeatureId==null) {
        val ats = f.getAttributes
        f.setFeatureId(ats.get("exon_number")+"-"+ats.get("transcript_id"))
        f
      }else f)
  
  /**
     * Saves only gene features
     * @param filePath
     * @param destination
     */
    def saveGenesOnly(filePath:String,destination:String) = {
      val features: RDD[Feature] =   loadFeatures(filePath).filter(f=>f.getFeatureType=="gene")
      features.adamParquetSave(destination)
    }
  
    def convertGTF(filePath:String,destination: String) = {
      val features: RDD[Feature] = loadFeatures(filePath)
      features.adamParquetSave(destination)
    }
  
    def convertGTF(filePath:String,destination: String, geneFeatures:RDD[Feature]) = {
      val features: RDD[Feature] =  loadFeatures(filePath) ++ geneFeatures
      features.adamParquetSave(destination)
    }
  
    def cutGenes(filePath:String,destination:String,ids:Set[String]) = loadFeatures(filePath)
        .filter{   case f=> ids.contains(f.getAttributes.get("gene_id"))     }
        .adamParquetSave(destination)

  /* ... new cell ... */

  val gf = loadFeatures(destination+"genes.adam").cache()
  convertGTF(samplesPath+"3.gtf",destination+"features_3.adam",gf)
  convertGTF(samplesPath+"4.gtf",destination+"features_4.adam",gf)
  convertGTF(samplesPath+"9.gtf",destination+"features_9.adam",gf)

  /* ... new cell ... */

  val ids =  sc.textFile(utrs+"ids.txt").collect().toSet
  cutGenes(destination+"features_3.adam",destination+"features_small_3.adam",ids)
  cutGenes(destination+"features_4.adam",destination+"features_small_4.adam",ids)
  cutGenes(destination+"features_9.adam",destination+"features_small_9.adam",ids)

  /* ... new cell ... */

  def loadTranscripts(filePath:String) = ac.loadFeatures(filePath).filter(f=>f.getFeatureType=="transcript")
  def loadUTRs(filePath:String) = ac.loadFeatures(filePath).filter(f=>f.getFeatureType=="UTR")
  def loadGenes(filePath:String) = ac.loadFeatures(filePath).filter(f=>f.getFeatureType=="gene")
  
  val (t3,t4,t9) = (
  loadTranscripts(destination+"features_small_3.adam"),
  loadTranscripts(destination+"features_small_4.adam"),
  loadTranscripts(destination+"features_small_9.adam")
    )
  val (g3,g4,g9) = (
  loadGenes(destination+"features_small_3.adam"),
  loadGenes(destination+"features_small_4.adam"),
  loadGenes(destination+"features_small_9.adam")
    )
  
  val (u3,u4,u9) = (
  loadUTRs(destination+"features_small_3.adam"),
  loadUTRs(destination+"features_small_4.adam"),
  loadUTRs(destination+"features_small_9.adam")  
  )

  /* ... new cell ... */

  import org.bdgenomics.adam.predicates.HighQualityReadPredicate
  def strand(str: Strand): Boolean = str match {
    case Strand.Forward     => true
    case Strand.Reverse     => false
    case Strand.Independent => true
  }
  
  
  lazy val fields = Seq(AlignmentRecordField.readName,
    AlignmentRecordField.readMapped,
    AlignmentRecordField.contig,
    AlignmentRecordField.primaryAlignment,
    AlignmentRecordField.start,
    AlignmentRecordField.end,
    AlignmentRecordField.mapq,
    AlignmentRecordField.sequence
  )
  lazy val projection =Projection(fields)
  val reads = "/home/antonkulaga/data/adam/"
  
  def load(filePath:String): RDD[AlignmentRecord] =  ac.loadAlignments(filePath,predicate = Some(classOf[HighQualityReadPredicate]),projection =Some(projection))

  /* ... new cell ... */

  import org.bdgenomics.adam.rich._
  import ReferenceMappingContext._
  import org.bdgenomics.adam.rdd._
  
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

  /* ... new cell ... */

  val (r3,r4,r9) = (load(reads+"3.adam"),load(reads+"4.adam"),load(reads+"9.adam"))
  val cs3 = saveCountsByGeneIdsAndLen(u3,r3,destination+"counts_UTRs_3.txt")

  /* ... new cell ... */
}
              