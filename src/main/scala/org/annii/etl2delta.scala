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

class ArgsETL2Delta(arguments: Seq[String]) extends ScallopConf(arguments) {
  val path = opt[String](required = true, descr = "path to VCF files to process")
  val path2save = opt[String](required = true, descr = "path to output delta dataset")
  val includeSamples = opt[Boolean](default = Some(false), descr = "include samples to GT columns")
  val htsjdk = opt[Boolean](default = Some(false), descr = "use htsjdk VCF parser")
  verify()
}

object etl2delta {
  def main(arguments: Array[String]) {
    val args = new ArgsETL2Delta(arguments)
    val delta_silver_path = args.path2save()

    // Spark init
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("io.projectglow.vcf.fastReaderEnabled", !args.htsjdk())
      .getOrCreate()

    // Glow init
    import io.projectglow.Glow
    val sparkGlow = Glow.register(spark, true) // see https://github.com/projectglow/glow/issues/362

    // etl to delta
    val df = sparkGlow
      .read
      .format("vcf")
      .option("includeSampleIds", args.includeSamples())
      .option("flattenInfoFields", "true")
      .option("validationStringency", "lenient") // warnings on malformed rows
      .load(args.path())

    println("\t *** schema")
    df.printSchema()
    df.show(2, false)

    val dfBiallelic = Glow.transform("split_multiallelics", df)

    dfBiallelic
      .write
      .mode("overwrite")
      .format("delta")
      .save(delta_silver_path)
  }
}