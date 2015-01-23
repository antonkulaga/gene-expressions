package org.denigma.genes.parsers


import java.io.File
import java.util.UUID
import org.bdgenomics.formats.avro._
import org.bdgenomics.formats.avro.{Contig, Strand, Feature}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.bdgenomics.adam.rdd.features._

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

    val (_id, _parentId) =
      feature match {
        case "gene"       => (attrs.get("gene_id"), None)
        case "transcript" => (attrs.get("transcript_id"), attrs.get("gene_id"))
        case "exon"       => (attrs.get("exon_id"), attrs.get("transcript_id"))
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