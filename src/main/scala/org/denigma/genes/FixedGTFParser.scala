package org.denigma.genes


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{Exon, UTR}
import org.bdgenomics.adam.projections.{AlignmentRecordField, Projection}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.features.{GTFParser, FeatureParser}
import org.bdgenomics.adam.rich.ReferenceMappingContext.FeatureReferenceMapping
import org.bdgenomics.formats.avro.{Contig, AlignmentRecord, Feature, Strand}

/**
 * GTF is a line-based GFF variant.
 *
 * Details of the GTF/GFF format here:
 * http://www.ensembl.org/info/website/upload/gff.html
 */
class FixedGTFParser extends FeatureParser {

  override def parse(line: String): Seq[Feature] = {
    // Just skip the '#' prefixed lines, these are comments in the
    // GTF file format.
    if (line.startsWith("#")) {
      return Seq()
    }

    val fields = line.split("\t")

    val (seqname, source, feature, start, end, score, strand, frame, attribute) =
      (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8))

    lazy val attrs = GTFParser.parseAttrs(attribute)

    val contig = Contig.newBuilder().setContigName(seqname).build()
    val f = Feature.newBuilder()
      .setContig(contig)
      .setStart(start.toLong - 1) // GTF/GFF ranges are 1-based
      .setEnd(end.toLong) // GTF/GFF ranges are closed
      .setFeatureType(feature)
      .setSource(source)

    val _strand = strand match {
      case "+" => Strand.Forward
      case "-" => Strand.Reverse
      case _   => Strand.Independent
    }
    f.setStrand(_strand)
    
    val exonId: Option[String] = attrs.get("exon_id").orElse{
      attrs.get("transcript_id").flatMap(t=>attrs.get("exon_number").map(e=>t+"_"+e))
      
    }
    val (_id, _parentId) =
      feature match {
        case "gene"       => (attrs.get("gene_id"), None)
        case "transcript" => (attrs.get("transcript_id"), attrs.get("gene_id"))
        case "exon"       => (exonId, attrs.get("transcript_id"))
        case "CDS"        => (attrs.get("id"), attrs.get("transcript_id"))
        case "UTR"        => (attrs.get("id"), attrs.get("transcript_id"))
        case _            => (attrs.get("id"), None)
      }
    _id.foreach(f.setFeatureId)
    _parentId.foreach(parentId => f.setParentIds(List[String](parentId)))

    f.setAttributes(attrs)

    Seq(f.build())
  }
}