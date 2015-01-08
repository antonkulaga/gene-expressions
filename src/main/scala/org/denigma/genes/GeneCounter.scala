package org.denigma.genes

import java.io.{ File, PrintWriter }

import org.apache.spark.SparkContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.features.GeneFeatureRDD._
import org.bdgenomics.adam.rdd.features.FeaturesContext._
import org.bdgenomics.formats.avro.Feature
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.core
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.Feature

case class GeneCounter(sc: SparkContext) {

  lazy val ac = new ADAMContext(sc)

  /**
   * Just a test method that reads gtf and prints something to a file
   * @param from
   * @param to
   */
  def printFeatures(from: String, to: String) = {
    import org.bdgenomics.adam.projections.FeatureField._
    val features: RDD[Feature] = sc.adamGTFFeatureLoad(from)
    val results: Array[Seq[String]] = features.map {
      case f =>
        Seq(
          "---------------------------",
          s"ID = ${f.getFeatureId}",
          s"OF TYPE ${f.getFeatureType}",
          s"AT ${f.getStrand.name()} STRAND",
          s"starts at ${f.getStart}",
          s"ends at ${f.getEnd}"
        )
    }.collect()

    val f = new File(to)
    val w = new PrintWriter(f)
    for {
      r <- results
      l <- r
    } {
      w.write(l)
    }
    w.close()
  }

  def compute() = {

  }
}
