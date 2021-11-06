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

class ArgsVCF2Delta(arguments: Seq[String]) extends ScallopConf(arguments) {
  // required
  val path = opt[String](required = true, descr = "path to VCF files to process")
  val path2save = opt[String](required = true, descr = "path to output delta dataset")
  // transformations
  val normalize = opt[Boolean](noshort = true, default = Some(false), descr = "normalize variants")
  val reference = opt[String](noshort = true, default = Some(""), descr = "path to reference genome (normalization only)")
  dependsOnAll(reference, List(normalize))
  dependsOnAll(normalize, List(reference))
  verify()
}

object vcf2delta {
  def main(arguments: Array[String]) {
    val args = new ArgsVCF2Delta(arguments)
    val delta_silver_path = args.path2save()

    // Spark init
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._

    // Glow init
    import io.projectglow.Glow
    Glow.register(spark)

    // etl
    val df_original = spark.read.format("vcf")
      .option("includeSampleIds", "false")
      .option("splitToBiallelic", "true")  // achtung!!
      .option("flattenInfoFields", "true")
      .load(args.path())

    // normalize
    val df_normalized =
      if (args.normalize())
        Glow.transform("normalize_variants", df_original, Map("reference_genome_path" -> args.reference()))
      else df_original

    df_normalized.printSchema()
    df_normalized.show(20, false)

    // explode/expand annotated
    val df_ann = df_normalized
      .withColumn("VEP", expr("explode(INFO_CSQ)"))
      .selectExpr("contigName",
        "start",
        "referenceAllele",
        "alternateAlleles",
        "INFO_AN",
        "INFO_AC",
        "INFO_AF",
        "genotypes.calls",
        "expand_struct(VEP)" // expected at least: VARIANT_CLASS|Feature_type|Consequence|IMPACT|BIOTYPE|CADD_RAW|CADD_PHRED
      )

    println("\t\t *** VEP variants")
    df_ann.printSchema()
    df_ann.show(20, false)

    val df = df_ann
      .select("contigName",
        "start",
        "referenceAllele",
        "alternateAlleles",
        "INFO_AN",
        "INFO_AC",
        "INFO_AF",
        "calls",
        "VARIANT_CLASS",
        "Feature_type",
        "Consequence",
        "IMPACT",
        "BIOTYPE",
        "SIFT",
        "PolyPhen",
        "CADD_RAW",
        "CADD_PHRED")

    println("\t\t *** Selected fields ")
    df.printSchema()
    df.show(20, false)

    df
      .write
      .mode("overwrite")
      .format("delta")
      .save(delta_silver_path)
  }
}