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

import org.annii.{Allele, isPseudoautosomalX, minRep, vType}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, WrappedArray}

object ParserRow {

  def parseRow(contig: String, start: Int, ref: String, alternateAlleles: WrappedArray[String], splitFromMultiAllelic: Boolean,
               calls: WrappedArray[WrappedArray[Int]], rsid: Int, qual:Float,  filter: String, info: String, format: String,
               nameNsex: Array[(String,String)], nameToId: mutable.Map[String, Int], minrep: Boolean, grch38: Boolean): ArrayBuffer[Allele] = {
    if (calls.length != nameNsex.length)
      throw new RuntimeException("ERROR: # samples in provided list should match # samples in row")

    val end = start + ref.length - 1
    val rows = ArrayBuffer[Allele]()
    val altz = alternateAlleles.zipWithIndex
    val altHom = new Array[Int](altz.length)
    val altHet = new Array[Int](altz.length)
    val altHomF = new Array[Int](altz.length)
    val altHetF = new Array[Int](altz.length)
    val altType = new Array[Byte](altz.length) // ASCII
    val altn = new Array[String](altz.length) // normalized alt
    val refn = new Array[String](altz.length) // normalized ref
    val posn = new Array[Int](altz.length) // normalized pos
    val wasn = new Array[Boolean](altz.length) // normalized flag
    var missed = 0
    var missedF = 0
    val X = if (contig == "X") true else false
    val Y = if (contig == "Y") true else false

    // infer type
    altz.foreach {
      case ("*", _) => // ignore upstream deletions
      case (alt, i) =>
        var r = ref
        var a = alt
        if (minrep) {
          val ras = minRep(ref, alt)
          r = ras._1
          a = ras._2
          refn(i) = r
          altn(i) = a
          posn(i) = start + ras._3
          wasn(i) = !r.equals(ref) || !a.equals(alt) || (ras._3 != 0)
        }
        altType(i) = vType(r, a)
    }

    // Samples
    calls
      .zipWithIndex
      .foreach {
        case (gt, sampleInd) =>
          if (gt.length != 2) throw new java.text.ParseException(s"Genotype is not a diploid at $contig-$start: '$gt'", 0)
          val dp: Int = 0 // todo: get from glow
        val female = nameNsex(sampleInd)._2.equals("female")

          if (gt(0) == -1 && gt(1) == -1) {
            if (X & female)
              missedF += 1
            else
              missed += 1
          } else {
            altz.foreach {
              case ("*", _) => // ignore upstream deletions
              case (alt, i) =>
                val altVCFIndx = i + 1 // index in vcf GT
                ((gt(0), gt(1)) match {
                  case (x, y) if x == altVCFIndx && y == altVCFIndx =>
                    if (X & female) altHomF(i) += 1 else altHom(i) += 1
                    Some("1/1", true)
                  case (x, _) if x == altVCFIndx =>
                    if (X & female) altHetF(i) += 1 else altHet(i) += 1
                    Some("1/_", false)
                  case (_, y) if y == altVCFIndx =>
                    if (X & female) altHetF(i) += 1 else altHet(i) += 1
                    Some("_/1", false)
                  case (_, _) => None
                }) match {
                  case Some(z) =>
                    if (minrep)
                      rows.append(Allele(nameToId(nameNsex(sampleInd)._1), contig, posn(i), posn(i) + refn(i).length - 1,
                        rsid, refn(i), altn(i), altType(i), wasn(i), splitFromMultiAllelic,
                        (i + 1).toShort, qual, filter, info, format, "", z._1, dp, z._2, 0, 0, 0, 0, 0, 0, 0))
                    else
                      rows.append(Allele(nameToId(nameNsex(sampleInd)._1), contig, start, end, rsid, ref, alt, altType(i),
                        minrep, splitFromMultiAllelic, (i + 1).toShort, qual, filter, info, format, "",
                        z._1, dp, z._2, 0, 0, 0, 0, 0, 0, 0))
                  case None =>
                }
            } // all alts
          }
      } // all samples

    // Stats - works for single load only
    if (X) {
      val isPAR = isPseudoautosomalX(start, end, grch38)
      val totalSf = nameNsex.count(x => x._2.equals("female"))
      val totalSm = nameNsex.length - totalSf
      val nsf = totalSf - missedF
      val nsm = totalSm - missed
      rows.foreach { v =>
        val i = v.vcfindex - 1
        val acm = if (isPAR) 2 * altHom(i) + altHet(i) else altHom(i) + (altHet(i).toFloat / 2)
        val acf = 2 * altHomF(i) + altHetF(i)
        v.homc = altHom(i)
        v.homfc = altHomF(i)
        v.hetc = altHet(i) // true het only in females
        v.hetfc = altHetF(i)
        v.ac = acm + acf
        v.an = if (isPAR) 2 * (nsf + nsm) else 2 * nsf + nsm
        if (v.an != 0) v.af = v.ac / v.an
      }
    } else if (Y) {
      rows.foreach { v =>
        v.homc = altHom(v.vcfindex - 1)
        v.hetc = altHet(v.vcfindex - 1)
        v.ac = v.homc + (v.hetc.toFloat / 2)
        v.an = nameNsex.length - missed
        if (v.an != 0) v.af = v.ac / v.an
      }
    } else {
      rows.foreach { v =>
        v.homc = altHom(v.vcfindex - 1)
        v.hetc = altHet(v.vcfindex - 1)
        v.ac = 2 * v.homc + v.hetc
        v.an = 2 * (nameNsex.length - missed)
        if (v.an != 0) v.af = v.ac / v.an
      }
    }

    rows
  }
}
