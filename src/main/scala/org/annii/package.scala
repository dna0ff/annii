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

package org

import scala.io.Source

package object annii {

  // no upper case for fields mapped to Kudu
  case class Allele(sample_id: Int, contig: String, start: Int, end: Int, rsid: Int, ref: String, alt: String, vtype: Byte,
                    norm: Boolean, multi: Boolean, vcfindex: Short, qual: Float, filter: String, info: String, format: String,
                    sample_data: String, gt: String, dp: Int, hom: Boolean, var af: Float, var ac: Float, var an: Int,
                    var homc: Int, var hetc: Int, var homfc: Int, var hetfc: Int)

  // Variants & GT are Kudu specific
  case class Variants(contig: String, start: Int, end: Int, ref: String, alt: String, rsid: Int, vtype: Byte, norm: Boolean,
                      multi: Boolean, af: Float, ac: Float, var an: Int, homc: Int, hetc: Int, homfc: Int, hetfc: Int, info: String)

  case class GT(sample_id: Int, contig: String, start: Int, end: Int, ref: String, alt: String, rsid: Int, vtype: Byte,
                vcfindex: Short, qual: Float, filter: String, format: String, sample_data: String, gt: String, dp: Int, hom: Boolean)

  case class Samples(sample_name: String, sample_id: Int, gender: String)

  /**
    * definitions: https://genome.sph.umich.edu/wiki/Variant_Normalization
    * discussion:  http://discuss.hail.is/t/variant-representation-normalization/343
    * @return trimmed alleles, no left extension as it would require reference genome
    */
  def rightLeftTrim(alt1: String, alt2: String): (String, String, Int) = {
    var suff = 1
    while (suff < alt2.length && alt1(alt1.length-suff) == alt2(alt2.length-suff)) suff += 1
    val a1 = alt1.substring(0, alt1.length-suff+1)
    val a2 = alt2.substring(0, alt2.length-suff+1)
    var pref = 0 // index of first unequal symbol
    while (pref < a2.length - 1 && a1(pref) == a2(pref)) pref += 1
    (a1.substring(pref), a2.substring(pref), pref)
  }

  def minRep(ref: String, alt: String): (String, String, Int) = {
    if (ref.equalsIgnoreCase(alt))
      return (ref.substring(0,1), alt.substring(0,1), 0)
    if (ref.length >= alt.length )
      return rightLeftTrim(ref,alt)
    val a = rightLeftTrim(alt,ref)
    (a._2, a._1, a._3)
  }

  // pseudoautosomal regions
  val Xp22Starts38 = 10001
  val Xp22Stops38 = 2781479
  val Xq28Starts38 = 155701383
  val Xq28Stops38 = 156030895
  val Xp22Starts37 = 60001
  val Xp22Stops37 = 2699520
  val Xq28Starts37 = 154931044
  val Xq28Stops37 = 155260560

  def inPAR1X(start: Int, end: Int, grch38: Boolean): Boolean =
    if (grch38) start >= Xp22Starts38 && end <= Xp22Stops38 else start >= Xp22Starts37 && end <= Xp22Stops37

  def inPAR2X(start: Int, end: Int, grch38: Boolean): Boolean =
    if (grch38) start >= Xq28Starts38 && end <= Xq28Stops38 else start >= Xq28Starts37 && end <= Xq28Stops37

  def isPseudoautosomalX(start: Int, end: Int, grch38: Boolean): Boolean = inPAR1X(start, end, grch38) || inPAR2X(start, end, grch38)

  // Simplified type classification
  // Depricated, saved for Kudu only
  def vType(r: String, a:String): Byte =
    if (math.abs(r.length - a.length) > 50) 's' // SV
    else if (r.length < a.length) {
      if (a.contains(r)) 'i' // Insertion
      else 'c' // Complex
    } else if (r.length > a.length) {
      if (r.contains(a)) 'd' // Deletion
      else 'c' // Complex
    } else if (r.length == 1) 'S' // SNV
    else 'M' // MNV

  def readSampleInfo(file: String): Array[(String,String)] =
  // don't want to catch any exceptions here, let them fail the program
    (for (line <- Source.fromFile(file).getLines()
          if line.split(",")(1).equalsIgnoreCase("male") ||
            line.split(",")(1).equalsIgnoreCase("female")
    ) yield (line.split(",")(0), line.split(",")(1).toLowerCase))
      .toArray

}