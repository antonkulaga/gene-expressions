package org.denigma.genes

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.predicates.HighQualityReadPredicate
import org.bdgenomics.adam.projections.{AlignmentRecordField, Projection}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.read.comparisons.ComparisonTraversalEngine
import org.bdgenomics.formats.avro.{AlignmentRecord, Strand}

trait ExpressionsBase {

  @transient val sc:SparkContext
  @transient val ac:ADAMContext

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

  /**
   * Projection to extract only properties we need*
   */
  lazy val projection =Projection(fields)

  def compare(a:RDD[AlignmentRecord],b:RDD[AlignmentRecord]) = new ComparisonTraversalEngine(fields,a,b)(sc)

  def load(filePath:String): RDD[AlignmentRecord] =  ac.loadAlignments(filePath,predicate = Some(classOf[HighQualityReadPredicate]),projection =Some(projection))



}
