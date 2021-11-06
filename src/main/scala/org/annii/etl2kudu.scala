/*
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2020 Dmitry Degrave
 *  Copyright (c) 2020 Garvan Institute of Medical Research
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package org.annii

import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.Row
import org.rogach.scallop.ScallopConf

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.annii.kudu.ParserRow.parseRow
import org.annii.kudu.KuduCustomTable

class ArgsETL2Kudu(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner("\n Kudu ETL options:\n")
  // required
  val kuduHost = opt[String](short = 'h', required = true, descr = "Kudu hostname")
  val dataset = opt[String](required = true, descr = "dataset name")
  val path = opt[String](required = true, descr = "folder with multi-sample VCF files to process (local or HDFS)")
  val sinfo = opt[String](required = true, descr = "path to csv file with sample names and gender")
  // optional
  val kuduPort = opt[Int](noshort = true, default = Some(7051), validate = (0<), descr = "Kudu port, default = 7051")
  val rfv  = opt[Int](noshort = true, default = Some(1), validate = (0<), descr = "replication factor for Variants table, default = 1")
  val rfs  = opt[Int](noshort = true, default = Some(1), validate = (0<), descr = "replication factor for Samples table, default = 1")
  val rfgt  = opt[Int](noshort = true, default = Some(1), validate = (0<), descr = "replication factor for GT table, default = 1")
  val tabletsV = opt[Int](noshort = true, default = Some(16), validate = (0<), descr = "# tablets for Variants table, default = 16")
  val tabletsS = opt[Int](noshort = true, default = Some(16), validate = (0<), descr = "# tablets for Samples table, default = 16")
  val tabletsGt = opt[Int](noshort = true, default = Some(16), validate = (0<), descr = "# tablets for GT table, default = 16")
  val shuffle = opt[Int](noshort = true, default = Some(100), validate = (0<), descr = "# shuffle partitions in Sparks, default = 100")
  val skipGT = opt[Boolean]("skip-gt", noshort = true, default = Some(false), descr = "skip writing to GT table")
  val minrep = opt[Boolean](noshort = true, default = Some(false), descr = "minimal representation of variants")
  val grch38 = opt[Boolean](noshort = true, default = Some(false), descr = "aligned to GRCh38; default=GRCh37")
  val htsjdk = opt[Boolean](default = Some(false), descr = "use htsjdk VCF parser")
  verify()
}

object etl2kudu {

  def main(arguments: Array[String]) {
    val args = new ArgsETL2Kudu(arguments)

    // Spark init
    import org.apache.spark.sql.SparkSession
    val spark =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("io.projectglow.vcf.fastReaderEnabled", !args.htsjdk())
        .getOrCreate()

    import spark.implicits._

    val gtTable = args.dataset() + "_gt"
    val variantTable = args.dataset() + "_variants"
    val sampleTable = args.dataset() + "_samples"
    val nameToId = mutable.Map.empty[String, Int]

    val kuduMaster = args.kuduHost() + ":" + args.kuduPort().toString
    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)

    if (!kuduContext.tableExists(variantTable)) {
      KuduCustomTable.createVariantTable(variantTable, kuduMaster, args.rfv(), args.tabletsV())
    }

    if (!args.skipGT() && !kuduContext.tableExists(gtTable)) {
      KuduCustomTable.createGTTable(gtTable, kuduMaster, args.rfgt(), args.tabletsGt())
    }

    // todo make it work in 1.9
    //    if (kuduContext.tableExists(sampleTable)) {
    //      sqlContext.read.options(Map("kudu.master" -> kuduMaster,"kudu.table" -> sampleTable))
    //        .format("kudu").load
    //        .select("sample_name","sample_id")
    //        .collect()
    //        .foreach(row => nameToId(row.getString(0)) = row.getInt(1))
    //    } else {
    //      KuduCustomTable.createSampleTable(sampleTable, kuduMaster, args.rfs(), args.tabletsS())
    //    }

    if (!kuduContext.tableExists(sampleTable)) {
      KuduCustomTable.createSampleTable(sampleTable, kuduMaster, args.rfs(), args.tabletsS())
    }

    var offsetId = nameToId.size
    val newSamples = new ListBuffer[Samples]()
    val sNames = Some(readSampleInfo(args.sinfo()))

    sNames.getOrElse(Array()).foreach { s =>
      if (!nameToId.contains(s._1)) { // add new unique samples
        newSamples += Samples(s._1, offsetId, s._2)
        nameToId(s._1) = offsetId
        offsetId += 1
      }
    }

    if (newSamples.nonEmpty) {
      val sampleDf = spark.sparkContext.parallelize(newSamples).toDF()
      kuduContext.upsertRows(sampleDf, sampleTable) // update existing rows - for repeated VCF loads or failed/repeated spark tasks
    }

    val gtCounter = spark.sparkContext.longAccumulator("GT counter") // estimation of upserted rows

    // Glow init
    import io.projectglow.Glow
    val sparkGlow = Glow.register(spark, true) // see https://github.com/projectglow/glow/issues/362

    val minrep = args.minrep() // to avoid ArgsETL serialization in RDDs
    val grch38 = args.grch38() // to avoid ArgsETL serialization in RDDs

    // data load
    val df = sparkGlow
      .read
      .format("vcf")
      .option("includeSampleIds", "false")
      .option("flattenInfoFields", "false")
      .option("validationStringency", "lenient") // warnings on malformed rows
      .load(args.path())

    val dfBiallelic = Glow.transform("split_multiallelics", df)

    val alleleDF = dfBiallelic
      .select($"contigName", $"start", $"referenceAllele", $"alternateAlleles", $"splitFromMultiAllelic", $"genotypes.calls")
      .flatMap { // RDD[Allele]
        case Row(contigName: String, start: Long, referenceAllele: String, alternateAlleles: mutable.WrappedArray[String],
        splitFromMultiAllelic: Boolean, calls: mutable.WrappedArray[mutable.WrappedArray[Int]]) =>
          try {
            parseRow(contigName, start.toInt + 1, // Glow counts from 0, VCF is 1-based
                     referenceAllele, alternateAlleles, splitFromMultiAllelic, calls, 0, 0f, "filter", "info", "format",
                     sNames.getOrElse(Array()), nameToId, minrep, grch38)
          } catch {
            case ex: java.text.ParseException => print("*** WARNING: " + ex.getMessage); new ArrayBuffer[Allele]
            case _: Throwable => new ArrayBuffer[Allele]
          }
      }

    // variant table
    val variantDF = alleleDF
      .map(v => Variants(v.contig, v.start, v.end, v.ref, v.alt, v.rsid, v.vtype, v.norm, v.multi,
        v.af, v.ac, v.an, v.homc, v.hetc, v.homfc, v.hetfc, ""))
      .toDF()

    kuduContext.upsertRows(variantDF, variantTable) // update existing rows - for repeated VCF loads or failed/repeated spark tasks

    // GT table
    if (!args.skipGT()) {
      val gtDF = alleleDF
        .map { v =>
          gtCounter.add(1)
          GT(v.sample_id, v.contig, v.start, v.end, v.ref, v.alt, v.rsid, v.vtype, v.vcfindex, v.qual, v.filter, v.format,
            v.sample_data, v.gt, v.dp, v.hom) }
        .toDF()

      kuduContext.upsertRows(gtDF, gtTable) // update existing rows - for repeated VCF loads or failed/repeated spark tasks
    }

    print("\n*** All Samples: "); nameToId.foreach(print(_))
    print("\n*** New Samples: "); newSamples.foreach(print(_))
    print(s"\n*** # all samples: ${nameToId.size} \n*** # new samples: ${newSamples.size}")
    println("\n*** GTs added: <= " + gtCounter.value)
  }
}