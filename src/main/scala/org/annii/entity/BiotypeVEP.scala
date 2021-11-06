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

package org.annii.entity

/**
  * VEP BIOTYPE: Biotype of transcript or regulatory feature
  * seems not to be defined formally, best effort:
  * https://asia.ensembl.org/info/genome/genebuild/biotypes.html
  */
object BiotypeVEP extends Enumeration {
  val processed_transcript = Value("processed_transcript")

  val lncrna = Value("lncrna")
  val antisense = Value("antisense")
  val macro_lncrna = Value("macro_lncrna")
  val non_coding = Value("non_coding")
  val retained_intron = Value("retained_intron")
  val sense_intronic = Value("sense_intronic")
  val sense_overlapping = Value("sense_overlapping")
  val lincrna = Value("lincrna")

  val ncrna = Value("ncrna")
  val mirna = Value("mirna")
  val miscrna = Value("miscrna")
  val pirna = Value("pirna")
  val rrna = Value("rrna")
  val sirna = Value("sirna")
  val snrna = Value("snrna")
  val snorna = Value("snorna")
  val trna = Value("trna")
  val vaultrna = Value("vaultrna")

  val protein_coding = Value("protein_coding")

  val pseudogene = Value("pseudogene")
  val ig_pseudogene = Value("ig_pseudogene")
  val polymorphic_pseudogene = Value("polymorphic_pseudogene")
  val processed_pseudogene = Value("processed_pseudogene")
  val transcribed_pseudogene = Value("transcribed_pseudogene")
  val translated_pseudogene = Value("translated_pseudogene")
  val unitary_pseudogene = Value("unitary_pseudogene")
  val unprocessed_pseudogene = Value("unprocessed_pseudogene")

  val readthrough = Value("readthrough")
  val stop_codon_readthrough = Value("stop_codon_readthrough")
  val tec = Value("tec")

  val tr_gene = Value("tr_gene")
  val tr_c_gene = Value("tr_c_gene")
  val tr_d_gene = Value("tr_d_gene")
  val tr_j_gene = Value("tr_j_gene")
  val tr_v_gene = Value("tr_v_gene")

  val ig_gene = Value("ig_gene")
  val ig_c_gene = Value("ig_c_gene")
  val ig_d_gene = Value("ig_d_gene")
  val ig_j_gene = Value("ig_j_gene")
  val ig_v_gene = Value("ig_v_gene")

  val nonsense_mediated_decay = Value("nonsense_mediated_decay")

  val other = Value("other")

  def withNameOrNone(s: String): Option[Value] = values.find(_.toString == s)
}