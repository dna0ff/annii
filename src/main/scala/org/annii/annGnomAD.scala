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

import org.rogach.scallop.ScallopConf

class ArgsAnn(arguments: Seq[String]) extends ScallopConf(arguments) {
  val deltapathvcf = opt[String](required = true, descr = "path to vcf delta dataset")
  val deltapathgnomad = opt[String](required = true, descr = "path to gmomad delta dataset")
  val path2save = opt[String](required = true, descr = "path to annotated delta dataset")
  verify()
}

object annGnomAD {
  def main(arguments: Array[String]) {
    val args = new ArgsAnn(arguments)

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

    val df1 = spark.read.format("delta").load(args.deltapathvcf()).alias("vcf")
    val df2 = spark.read.format("delta").load(args.deltapathgnomad()).alias("gnomad")

    val df = df1
        .join(df2)
        .where(
          $"vcf.contigName" === $"gnomad.contigName"
          && $"vcf.start" === $"gnomad.start"
          && $"vcf.referenceAllele" === $"gnomad.referenceAllele"
          && $"vcf.alternateAlleles" === $"gnomad.alternateAlleles"
        )
        .select(
          "vcf.contigName",
          "vcf.start",
          "vcf.referenceAllele",
          "vcf.alternateAlleles",
          "INFO_AF_raw",
          "INFO_AF_afr",
          "INFO_AF_amr",
          "INFO_AF_asj",
          "INFO_AF_eas",
          "INFO_AF_fin",
          "INFO_AF_nfe",
          "INFO_AF_oth"
        )

    df
      .write
      .mode("overwrite")
      .format("delta")
      .save(args.path2save())

    println("\t *** annotated schema")
    df.printSchema()
    df.show(20, false)
  }
}