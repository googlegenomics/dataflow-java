/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dataflow.utils;

import com.google.api.client.util.Preconditions;
import com.google.api.services.genomics.model.Annotation;
import com.google.api.services.genomics.model.TranscriptExon;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

import htsjdk.samtools.util.SequenceUtil;

import java.util.Map;
import java.util.logging.Logger;

/**
 * Utilities for dealing with genomics {@link Annotation}s. Provides some very
 * basic functions for determining the effect of a given mutation on a
 * transcript.
 */
public class AnnotationUtils {
  private static final Logger LOG = Logger.getLogger(AnnotationUtils.class.getName());
  private static final char STOP = '*';
  private static final Map<String, Character> AMINO_ACIDS =
      ImmutableMap.<String, Character>builder()
        .put("TTT", 'F').put("TTC", 'F').put("TTA", 'L').put("TTG", 'L')
        .put("TCT", 'S').put("TCC", 'S').put("TCA", 'S').put("TCG", 'S')
        .put("TAT", 'Y').put("TAC", 'Y').put("TAA", STOP).put("TAG", STOP)
        .put("TGT", 'C').put("TGC", 'C').put("TGA", STOP).put("TGG", 'W')
        .put("CTT", 'L').put("CTC", 'L').put("CTA", 'L').put("CTG", 'L')
        .put("CCT", 'P').put("CCC", 'P').put("CCA", 'P').put("CCG", 'P')
        .put("CAT", 'H').put("CAC", 'H').put("CAA", 'Q').put("CAG", 'Q')
        .put("CGT", 'R').put("CGC", 'R').put("CGA", 'R').put("CGG", 'R')
        .put("ATT", 'I').put("ATC", 'I').put("ATA", 'I').put("ATG", 'M')
        .put("ACT", 'T').put("ACC", 'T').put("ACA", 'T').put("ACG", 'T')
        .put("AAT", 'N').put("AAC", 'N').put("AAA", 'K').put("AAG", 'K')
        .put("AGT", 'S').put("AGC", 'S').put("AGA", 'R').put("AGG", 'R')
        .put("GTT", 'V').put("GTC", 'V').put("GTA", 'V').put("GTG", 'V')
        .put("GCT", 'A').put("GCC", 'A').put("GCA", 'A').put("GCG", 'A')
        .put("GAT", 'D').put("GAC", 'D').put("GAA", 'E').put("GAG", 'E')
        .put("GGT", 'G').put("GGC", 'G').put("GGA", 'G').put("GGG", 'G').build();

  private AnnotationUtils() {}

  /**
   * Describes the effect of a given variant on a transcript. Subset of the
   * annotations API field {@code VariantAnnotation.effect}.
   */
  public enum VariantEffect { SYNONYMOUS_SNP, STOP_GAIN, STOP_LOSS, NONSYNONYMOUS_SNP }

  /**
   * Determines the effect of the given allele at the given position within a
   * transcript. Currently, this routine is relatively primitive: it only works
   * with SNPs and only computes effects on coding regions of the transcript.
   * This utility currently does not handle any splice sites well, including
   * splice site disruption and codons which span two exons.
   *
   * @param variantStart 0-based absolute start for the variant.
   * @param allele Bases to substitute at {@code variantStart}.
   * @param transcript The affected transcript.
   * @param transcriptBases The reference bases which span the provided
   *     {@code transcript}. Must match the exact length of the
   *     {@code transcript.position}.
   * @return The effect of this variant on the given transcript, or null if
   *     unknown.
   */
  public static VariantEffect determineVariantTranscriptEffect(
      long variantStart, String allele, Annotation transcript, String transcriptBases) {
    long txLen = transcript.getPosition().getEnd() - transcript.getPosition().getStart();
    Preconditions.checkArgument(transcriptBases.length() == txLen,
        "transcriptBases must have equal length to the transcript; got " +
            transcriptBases.length() + " and " + txLen + ", respectively");
    if (allele.length() != 1) {
      LOG.fine("determineVariantTranscriptEffects() only supports SNPs, ignoring " + allele);
      return null;
    }
    if (transcript.getTranscript().getCodingSequence() == null) {
      LOG.fine("determineVariantTranscriptEffects() only supports intersection with coding " +
          "transcript, ignoring ");
      return null;
    }

    long variantEnd = variantStart + 1;
    Range<Long> variantRange = Range.closedOpen(variantStart, variantEnd);
    Range<Long> codingRange = Range.closedOpen(
        transcript.getTranscript().getCodingSequence().getStart(),
        transcript.getTranscript().getCodingSequence().getEnd());
    if (Boolean.TRUE.equals(transcript.getPosition().getReverseStrand())) {
      allele = SequenceUtil.reverseComplement(allele);
    }
    for (TranscriptExon exon : transcript.getTranscript().getExons()) {
      // For now, only compute effects on variants within the coding region of an exon.
      Range<Long> exonRange = Range.closedOpen(exon.getStart(), exon.getEnd());
      if (exonRange.isConnected(codingRange) &&
          exonRange.intersection(codingRange).isConnected(variantRange) &&
          !exonRange.intersection(codingRange).intersection(variantRange).isEmpty()) {
        // Get the bases which correspond to this exon.
        int txOffset = transcript.getPosition().getStart().intValue();
        String exonBases = transcriptBases.substring(
            exon.getStart().intValue() - txOffset, exon.getEnd().intValue() - txOffset);
        int variantExonOffset = (int) (variantStart - exon.getStart());

        if (Boolean.TRUE.equals(transcript.getPosition().getReverseStrand())) {
          // Normalize the offset and bases to 5' -> 3'.
          exonBases = SequenceUtil.reverseComplement(exonBases);
          variantExonOffset = (int) (exon.getEnd() - variantEnd);
        }

        // Determine the indices for the codon which contains this variant.
        if (exon.getFrame() == null) {
          LOG.fine("exon lacks frame data, cannot determine effect");
          return null;
        }
        int offsetWithinCodon = (variantExonOffset + exon.getFrame().getValue()) % 3;
        int codonExonOffset = variantExonOffset - offsetWithinCodon;
        if (codonExonOffset < 0 || exonBases.length() <= codonExonOffset+3) {
          LOG.fine("variant codon spans multiple exons, this case is not yet handled");
          return null;
        }
        String fromCodon = exonBases.substring(codonExonOffset, codonExonOffset+3);
        String toCodon =
            fromCodon.substring(0, offsetWithinCodon) + allele +
            fromCodon.substring(offsetWithinCodon + 1);
        return codonChangeToEffect(fromCodon, toCodon);
      }
    }
    return null;
  }

  private static VariantEffect codonChangeToEffect(String fromCodon, String toCodon) {
    Preconditions.checkArgument(AMINO_ACIDS.containsKey(fromCodon), "no such codon: " + fromCodon);
    Preconditions.checkArgument(AMINO_ACIDS.containsKey(toCodon), "no such codon: " + toCodon);
    char fromAA = AMINO_ACIDS.get(fromCodon);
    char toAA = AMINO_ACIDS.get(toCodon);
    if (fromAA == toAA) {
      return VariantEffect.SYNONYMOUS_SNP;
    } else if (toAA == STOP) {
      return VariantEffect.STOP_GAIN;
    } else if (fromAA == STOP) {
      return VariantEffect.STOP_LOSS;
    } else {
      return VariantEffect.NONSYNONYMOUS_SNP;
    }
  }
}
