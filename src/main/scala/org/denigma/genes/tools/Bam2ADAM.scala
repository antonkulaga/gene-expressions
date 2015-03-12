package org.denigma.genes.tools

import java.io.File
import java.util.concurrent._

import htsjdk.samtools._
import org.apache.hadoop.fs.Path
import org.bdgenomics.adam.converters.SAMRecordConverter
import org.bdgenomics.adam.models.{RecordGroupDictionary, SequenceDictionary}
import org.bdgenomics.formats.avro.AlignmentRecord
import parquet.avro.AvroParquetWriter
import parquet.hadoop.metadata.CompressionCodecName

import scala.collection.JavaConversions._

/**
 * * Mostly copy-paste from ADAM console convertion command
 * * I use it just because it is easier for me to run such conversion from the code rather than
 * * from ADAM console
 * @param bamFile
 * @param outputPath
 * @param qSize
 */
class Bam2ADAM(val bamFile:String, val outputPath:String,qSize:Int = 10000)
{

  val blockSize = 128 * 1024 * 1024
  val pageSize = 1 * 1024 * 1024
  val compressionCodec = CompressionCodecName.GZIP
  val disableDictionaryEncoding = false
  var numThreads = 4

  val blockingQueue = new LinkedBlockingQueue[Option[(SAMRecord, SequenceDictionary, RecordGroupDictionary)]](qSize)
  
  def fileName(num:Int) = outputPath + "/part%d".format(num)

  val writerThreads = (0 until numThreads).foldLeft(List[Thread]()) {
    (list, threadNum) =>
      {
        val writerThread = new Thread(new Runnable {

          val parquetWriter = new AvroParquetWriter[AlignmentRecord](
            new Path(outputPath + "/part%d".format(threadNum)),
            AlignmentRecord.SCHEMA$,
            compressionCodec,
            blockSize,
            pageSize,
            disableDictionaryEncoding
          )

          val samRecordConverter = new SAMRecordConverter

          def run(): Unit = {
            try {
              while (true) {
                blockingQueue.take() match {
                  // Poison Pill
                  case None =>
                    // Close my parquet writer
                    parquetWriter.close()
                    // Notify other threads
                    blockingQueue.add(None)
                    // Exit
                    return
                  case Some((samRecord, seqDict, rgDict)) =>
                    parquetWriter.write(samRecordConverter.convert(samRecord, seqDict, rgDict))
                }
              }
            } catch {
              case ie: InterruptedException =>
                Thread.interrupted()
            }
          }
        })
        writerThread.setName("ADAMWriter%d".format(threadNum))
        writerThread
      } :: list
  }

  def run() = {

    val samReader = new SAMFileReader(new File(bamFile), null, true)
    samReader.setValidationStringency(ValidationStringency.LENIENT)

    val seqDict = SequenceDictionary(samReader)
    val rgDict = RecordGroupDictionary.fromSAMReader(samReader)

    println(seqDict)

    writerThreads.foreach(_.start())
    var i = 0
    for (samRecord <- samReader) {
      i += 1
      blockingQueue.put(Some((samRecord, seqDict, rgDict)))
      if (i % 1000000 == 0) {
        println("***** Read %d million reads from SAM/BAM file (queue=%d) *****".format(i / 1000000, blockingQueue.size()))
      }
    }
    blockingQueue.put(None)
    samReader.close()
    println("Waiting for writers to finish")
    writerThreads.foreach(_.join())
    System.err.flush()
    System.out.flush()
    println("\nFinished! Converted %d reads total.".format(i))
  }
}

