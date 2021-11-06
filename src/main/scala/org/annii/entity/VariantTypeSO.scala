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
  * Sequence Ontology Variant classes
  * http://www.sequenceontology.org/
  * https://asia.ensembl.org/info/genome/variation/prediction/classification.html#classes
  */
object VariantTypeSO extends Enumeration {
  val snv = Value("snv")
  val insertion = Value("insertion")
  val deletion = Value("deletion")
  val indel = Value("indel")
  val substitution = Value("substitution")
  val inversion = Value("inversion")
  val translocation = Value("translocation")
  val duplication = Value("duplication")
  val alu_insertion = Value("alu_insertion")
  val complex_structural_alteration = Value("complex_structural_alteration")
  val complex_substitution = Value("complex_substitution")
  val copy_number_gain = Value("copy_number_gain")
  val copy_number_loss = Value("copy_number_loss")
  val copy_number_variation = Value("copy_number_variation")
  val interchromosomal_breakpoint = Value("interchromosomal_breakpoint")
  val interchromosomal_translocation = Value("interchromosomal_translocation")
  val intrachromosomal_breakpoint = Value("intrachromosomal_breakpoint")
  val intrachromosomal_translocation = Value("intrachromosomal_translocation")
  val loss_of_heterozygosity = Value("loss_of_heterozygosity")
  val mobile_element_deletion = Value("mobile_element_deletion")
  val mobile_element_insertion = Value("mobile_element_insertion")
  val novel_sequence_insertion = Value("novel_sequence_insertion")
  val short_tandem_repeat_variation = Value("short_tandem_repeat_variation")
  val tandem_duplication = Value("tandem_duplication")
  val probe = Value("probe")
  val other = Value("other")

  def withNameOrNone(s: String): Option[Value] = values.find(_.toString == s)
}