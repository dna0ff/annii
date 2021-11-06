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

class ArgsGnomad2Delta(arguments: Seq[String]) extends ScallopConf(arguments) {
  val path = opt[String](required = true, descr = "path to gnomad VCF files")
  val path2save = opt[String](required = true, descr = "path to output delta dataset")
  verify()
}

object gnomAD2delta {
  def main(arguments: Array[String]) {
    val args = new ArgsGnomad2Delta(arguments)
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

    // etl to delta
    val df = spark.read.format("vcf")
      .option("includeSampleIds", "true") // must be true, otherwise exception on saving to delta as gmomad VCFs do not have samples
//      .option("splitToBiallelic", "true") /// todo not supported in glow .6 - use transformer
      .option("flattenInfoFields", "true")
      .load(args.path())
      .write
      .mode("overwrite")
      .format("delta")
      .save(delta_silver_path)
  }
}