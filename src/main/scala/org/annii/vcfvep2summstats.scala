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

import org.apache.spark.sql.functions.expr
import org.rogach.scallop.ScallopConf

class ArgsVCF2summstats(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner(s"\n Converts VEP annotated VCF to summary stats tsv. Options:\n")
  // required
  val path = opt[String](required = true, descr = "path to VCF files to process")
  val path2save = opt[String](required = true, descr = "final destination")
  // transformations
  val normalize = opt[Boolean](noshort = true, default = Some(false), descr = "normalize variants")
  val reference = opt[String](noshort = true, default = Some(""), descr = "path to reference genome (normalization only)")
  // optional annotations
  val gnomad = opt[String](noshort = true, descr = "extract gnomAD AF annotations with provided prefix " +
    "(default prefix in VEP is gnomAD_AF if --af_gnomad was used in VEP)")
  val clinsig = opt[String](noshort = true, descr = "extract ClinVar Clinical significance annotations with provided " +
    "prefix (default prefix in VEP is CLIN_SIG if --clin_sig_allele was used in VEP)")
  val cadd = opt[Boolean](noshort = true, default = Some(false), descr = "extract CADD_RAW/CADD_PHRED annotations")
  dependsOnAll(reference, List(normalize))
  dependsOnAll(normalize, List(reference))
  verify()
}

object vcfvep2summstats {
  def main(arguments: Array[String]) {
    val args = new ArgsVCF2summstats(arguments)
    val gnomad = args.gnomad.getOrElse("")
    val clinsig = args.clinsig.getOrElse("")
    val cadd = args.cadd()

    // Spark init
    import org.apache.spark.sql.SparkSession
    val spark =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()

    import spark.implicits._

    // Glow init
    import io.projectglow.Glow
    Glow.register(spark)

    // etl
    val df_original =
      spark
        .read
        .format("vcf")
        .option("includeSampleIds", "false")
        .option("flattenInfoFields", "true")
        .option("validationStringency", "lenient") // warnings on malformed rows
        .load(args.path())

    // normalize
    val df_maybe_normalized =
      if (args.normalize())
        Glow.transform("normalize_variants", df_original, Map("reference_genome_path" -> args.reference()))
      else df_original

    if (args.normalize()) println("\t\t *** Original Normalized") else println("\t\t *** Original non-normalized")
    df_maybe_normalized.printSchema()
    df_maybe_normalized.show(2, false)

    // explode/expand annotated
    val df_vep =
      df_maybe_normalized
        .withColumn("VEP", expr("explode(INFO_CSQ)"))
        .selectExpr("contigName",
          "start",
          "referenceAllele",
          "alternateAlleles",
          "INFO_AN",
          "INFO_AC",
          "INFO_AF",
  //        "genotypes.calls",  // do not need until hetc/homc calculation
          "expand_struct(VEP)" // must have: VARIANT_CLASS|Feature_type|Consequence|IMPACT|BIOTYPE
        )

    println("\t\t *** VEP annotations")
    df_vep.printSchema()
    df_vep.show(20, false)

    val df = (cadd, gnomad, clinsig) match {
      case (false, "", "") =>
        df_vep
          .select("contigName",
            "start",
            "referenceAllele",
            "alternateAlleles",
            "INFO_AN",
            "INFO_AC",
            "INFO_AF",
            //            "calls", // do not need until hetc/homc calculation
            "VARIANT_CLASS",
            "Feature_type",
            "Consequence",
            "IMPACT",
            "BIOTYPE",
            "SIFT",
            "PolyPhen")

      case (true, "", "") =>
        df_vep
          .select("contigName",
            "start",
            "referenceAllele",
            "alternateAlleles",
            "INFO_AN",
            "INFO_AC",
            "INFO_AF",
            //            "calls", // do not need until hetc/homc calculation
            "VARIANT_CLASS",
            "Feature_type",
            "Consequence",
            "IMPACT",
            "BIOTYPE",
            "SIFT",
            "PolyPhen",
            "CADD_RAW",
            "CADD_PHRED")

      case (false, gnomad, "") =>
        df_vep
          .select("contigName",
            "start",
            "referenceAllele",
            "alternateAlleles",
            "INFO_AN",
            "INFO_AC",
            "INFO_AF",
            //            "calls", // do not need until hetc/homc calculation
            "VARIANT_CLASS",
            "Feature_type",
            "Consequence",
            "IMPACT",
            "BIOTYPE",
            "SIFT",
            "PolyPhen",
            gnomad)

      case (true, gnomad, "") =>
        df_vep
          .select("contigName",
            "start",
            "referenceAllele",
            "alternateAlleles",
            "INFO_AN",
            "INFO_AC",
            "INFO_AF",
            //            "calls", // do not need until hetc/homc calculation
            "VARIANT_CLASS",
            "Feature_type",
            "Consequence",
            "IMPACT",
            "BIOTYPE",
            "SIFT",
            "PolyPhen",
            "CADD_RAW",
            "CADD_PHRED",
            gnomad)

      case (false, "", clin) =>
        df_vep
          .select("contigName",
            "start",
            "referenceAllele",
            "alternateAlleles",
            "INFO_AN",
            "INFO_AC",
            "INFO_AF",
            //            "calls", // do not need until hetc/homc calculation
            "VARIANT_CLASS",
            "Feature_type",
            "Consequence",
            "IMPACT",
            "BIOTYPE",
            "SIFT",
            "PolyPhen",
            clin)

      case (true, "", clin) =>
        df_vep
          .select("contigName",
            "start",
            "referenceAllele",
            "alternateAlleles",
            "INFO_AN",
            "INFO_AC",
            "INFO_AF",
            //            "calls", // do not need until hetc/homc calculation
            "VARIANT_CLASS",
            "Feature_type",
            "Consequence",
            "IMPACT",
            "BIOTYPE",
            "SIFT",
            "PolyPhen",
            "CADD_RAW",
            "CADD_PHRED",
            clin)

      case (false, gnomad, clin) =>
        df_vep
          .select("contigName",
            "start",
            "referenceAllele",
            "alternateAlleles",
            "INFO_AN",
            "INFO_AC",
            "INFO_AF",
            //            "calls", // do not need until hetc/homc calculation
            "VARIANT_CLASS",
            "Feature_type",
            "Consequence",
            "IMPACT",
            "BIOTYPE",
            "SIFT",
            "PolyPhen",
            gnomad,
            clin)

      case (true, gnomad, clin) =>
        df_vep
          .select("contigName",
            "start",
            "referenceAllele",
            "alternateAlleles",
            "INFO_AN",
            "INFO_AC",
            "INFO_AF",
            //            "calls", // do not need until hetc/homc calculation
            "VARIANT_CLASS",
            "Feature_type",
            "Consequence",
            "IMPACT",
            "BIOTYPE",
            "SIFT",
            "PolyPhen",
            "CADD_RAW",
            "CADD_PHRED",
            gnomad,
            clin)
    }

    println("\t\t *** Selected fields ")
    df.printSchema()
    df.show(2, false)

    import org.apache.spark.sql.functions._

    val seqStr2str = udf((vs: Seq[String]) => vs match {
      case null => null
      case _    => s"""${vs.mkString(",")}"""
    })

    val seqInt2str = udf((vs: Seq[Int]) => vs match {
      case null => null
      case _    => s"""${vs.mkString(",")}"""
    })

    val seqDbl2str = udf((vs: Seq[Double]) => vs match {
      case null => null
      case _    => s"""${vs.mkString(",")}"""
    })

    df
      .withColumn("start", col("start") + 1)
      .withColumn("alternateAlleles", seqStr2str($"alternateAlleles"))
//      .withColumn("INFO_AN", seqInt2string($"INFO_AN"))
      .withColumn("INFO_AC", seqInt2str($"INFO_AC"))
      .withColumn("INFO_AF", seqDbl2str($"INFO_AF"))
      .withColumn("Consequence", seqStr2str($"Consequence"))
      .coalesce(1) // collect on single node and save in single file
      .distinct
      .write
      .mode("overwrite")
      .option("header","true")
      .option("sep","\t")
      .format("com.databricks.spark.csv")
      .save(args.path2save())
  }
}