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
package org.annii.kudu

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.{CreateTableOptions, KuduClient}
import org.apache.kudu.{ColumnSchema, Schema, Type}

import scala.collection.JavaConverters._

/**
  * Verbose way to create Kudu tables, as kuduContext doesn't handle columns encoding
  */
object KuduCustomTable {

  def createVariantTable(name: String, host: String, rf: Int, buckets: Int) = {
    val vopt = new CreateTableOptions().setNumReplicas(rf).addHashPartitions(List("contig", "start", "end", "ref", "alt").asJava, buckets)
    new KuduClient.KuduClientBuilder(host).build()
      .createTable(name, variantTableSchema(), vopt)
  }

  def createGTTable(name: String, host: String, rf: Int, buckets: Int) = {
    val gtopt = new CreateTableOptions().setNumReplicas(rf).addHashPartitions(List("sample_id", "contig", "start", "end", "ref", "alt").asJava, buckets)
    new KuduClient.KuduClientBuilder(host).build()
      .createTable(name, gtTableSchema(), gtopt)
  }

  def createSampleTable(name: String, host: String, rf: Int, buckets: Int) = {
    val sopt = new CreateTableOptions().setNumReplicas(rf).addHashPartitions(List("sample_name").asJava, buckets)
    new KuduClient.KuduClientBuilder(host).build()
      .createTable(name, samplesTableSchema(), sopt)
  }

  def variantTableSchema() = // alternative would be df.schema, but it doesn't handle columns encoding
    new Schema(
      List(
        new ColumnSchemaBuilder("contig", Type.STRING).key(true).nullable(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("start", Type.INT32).key(true).nullable(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("end", Type.INT32).key(true).nullable(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("ref", Type.STRING).key(true).nullable(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("alt", Type.STRING).key(true).nullable(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("rsid", Type.INT32).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("vtype", Type.INT8).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("norm", Type.BOOL).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("multi", Type.BOOL).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("af", Type.FLOAT).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("ac", Type.FLOAT).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("an", Type.INT32).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("homc", Type.INT32).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("hetc", Type.INT32).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("homfc", Type.INT32).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("hetfc", Type.INT32).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("info", Type.STRING).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build()
      ).asJava
    )

  def gtTableSchema() = // alternative would be df.schema, but it doesn't handle columns encoding
    new Schema(
      List(
        new ColumnSchemaBuilder("sample_id", Type.INT32).key(true).nullable(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("contig", Type.STRING).key(true).nullable(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("start", Type.INT32).key(true).nullable(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("end", Type.INT32).key(true).nullable(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("ref", Type.STRING).key(true).nullable(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("alt", Type.STRING).key(true).nullable(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("rsid", Type.INT32).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("vtype", Type.INT8).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("vcfindex", Type.INT16).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("qual", Type.FLOAT).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("filter", Type.STRING).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("format", Type.STRING).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("sample_data", Type.STRING).key(false).encoding(ColumnSchema.Encoding.PREFIX_ENCODING).build(), // gives 10% gain
        new ColumnSchemaBuilder("gt", Type.STRING).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("dp", Type.INT32).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("hom", Type.BOOL).key(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build()
      ).asJava
    )

  def samplesTableSchema() = // alternative would be df.schema, but it doesn't handle columns encoding
    new Schema(
      List(
        new ColumnSchemaBuilder("sample_name", Type.STRING).key(true).nullable(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("sample_id", Type.INT32).key(true).nullable(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build(),
        new ColumnSchemaBuilder("gender", Type.STRING).key(true).nullable(false).encoding(ColumnSchema.Encoding.AUTO_ENCODING).build()
      ).asJava
    )
}