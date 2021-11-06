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
  * VEP calculated variant consequences in SO terms
  * https://asia.ensembl.org/info/genome/variation/prediction/predicted_data.html#consequences
  * in order of severity (more severe to less severe)
  */
object ConsequenceSO extends Enumeration {
  val transcript_ablation = Value("transcript_ablation")
  val splice_acceptor_variant = Value("splice_acceptor_variant")
  val splice_donor_variant = Value("splice_donor_variant")
  val stop_gained = Value("stop_gained")
  val frameshift_variant = Value("frameshift_variant")
  val stop_lost = Value("stop_lost")
  val start_lost = Value("start_lost")
  val transcript_amplification = Value("transcript_amplification")
  val inframe_insertion = Value("inframe_insertion")
  val inframe_deletion = Value("inframe_deletion")
  val missense_variant = Value("missense_variant")
  val protein_altering_variant = Value("protein_altering_variant")
  val splice_region_variant = Value("splice_region_variant")
  val incomplete_terminal_codon_variant = Value("incomplete_terminal_codon_variant")
  val start_retained_variant = Value("start_retained_variant")
  val stop_retained_variant = Value("stop_retained_variant")
  val synonymous_variant = Value("synonymous_variant")
  val coding_sequence_variant = Value("coding_sequence_variant")
  val mature_miRNA_variant = Value("mature_mirna_variant")
  val five_prime_UTR_variant = Value("5_prime_utr_variant")
  val three_prime_UTR_variant = Value("3_prime_utr_variant")
  val non_coding_transcript_exon_variant = Value("non_coding_transcript_exon_variant")
  val intron_variant = Value("intron_variant")
  val nmd_transcript_variant = Value("nmd_transcript_variant")
  val non_coding_transcript_variant = Value("non_coding_transcript_variant")
  val upstream_gene_variant = Value("upstream_gene_variant")
  val downstream_gene_variant = Value("downstream_gene_variant")
  val tfbs_ablation = Value("tfbs_ablation")
  val tfbs_amplification = Value("tfbs_amplification")
  val tf_binding_site_variant = Value("tf_binding_site_variant")
  val regulatory_region_ablation = Value("regulatory_region_ablation")
  val regulatory_region_amplification = Value("regulatory_region_amplification")
  val feature_elongation = Value("feature_elongation")
  val regulatory_region_variant = Value("regulatory_region_variant")
  val feature_truncation = Value("feature_truncation")
  val intergenic_variant = Value("intergenic_variant")
  val other = Value("other")

  def withNameOrNone(s: String): Option[Value] = values.find(_.toString == s)
}