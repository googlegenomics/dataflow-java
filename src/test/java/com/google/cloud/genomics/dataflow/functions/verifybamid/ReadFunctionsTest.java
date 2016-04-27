/*
 * Copyright 2015 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.genomics.dataflow.functions.verifybamid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.genomics.dataflow.model.ReadBaseQuality;
import com.google.cloud.genomics.dataflow.model.ReadBaseWithReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.genomics.v1.CigarUnit;
import com.google.genomics.v1.CigarUnit.Operation;
import com.google.genomics.v1.LinearAlignment;
import com.google.genomics.v1.Position;
import com.google.genomics.v1.Read;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests for the ReadUtils class.
 */
@RunWith(JUnit4.class)
public class ReadFunctionsTest {

  /**
   * Test whether a char is a valid base (ACGT)
   */
  private static boolean isBase(char c) {
    return c == 'A' || c == 'C' || c == 'G' || c == 'T';
  }

  static class CigarTestCase {

    /**
     * All tests are on 'chr1' for now (it doesn't matter).
     */
    private static final String CHROMOSOME = "1";

    /**
     * All read alignments start at position 10000 (it doesn't matter).
     */
    private static final Integer INITIAL_OFFSET = 10000;

    /**
     * String for a regular expression for a single CIGAR chunk.
     */
    private static final String CIGAR_CHUNK = "[0-9]+[MIDNSHPX=]";

    /**
     * Regular expression for a single CIGAR chunk.
     */
    private static final Pattern CIGAR_CHUNK_PATTERN = Pattern.compile(CIGAR_CHUNK);

    /**
     * Convert from string to Operation.
     */
    private static final Map<String, Operation> CIGAR_UNIT_OPERATION
        = new ImmutableMap.Builder<String, Operation>()
        .put("M", Operation.ALIGNMENT_MATCH)
        .put("H", Operation.CLIP_HARD)
        .put("S", Operation.CLIP_SOFT)
        .put("D", Operation.DELETE)
        .put("I", Operation.INSERT)
        .put("P", Operation.PAD)
        .put("=", Operation.SEQUENCE_MATCH)
        .put("X", Operation.SEQUENCE_MISMATCH)
        .put("N", Operation.SKIP)
        .build();

    final char[] alignedRef;
    final char[] alignedSeq;
    final String cigar;
    final Read read;
    final List<ReadBaseWithReference> readBases;

    /**
     * Builds a test case containing a read that can be passed to the Cigar class and the expected
     * read base results.
     *
     * @param seq The actual read sequence
     * @param qualityScores The list of quality scores, one for each read base
     * @param cigar The cigar string
     * @param alignedRef The reference sequence adjusted to be aligned
     * @param alignedSeq The read sequence adjusted to be aligned
     * @param refSeqs One sequence for each piece of the cigar string, corresponding to the
     * reference sequence for each piece
     * @param refSeqStart Where on the reference sequence the read starts
     */
    CigarTestCase(String seq, List<Integer> qualityScores, String cigar,
        String alignedRef, String alignedSeq, List<String> refSeqs,
        Integer refSeqStart) {

      if (alignedRef == null) {
        this.alignedRef = null;
      } else {
        this.alignedRef = alignedRef.toCharArray();
      }
      if (alignedSeq == null) {
        this.alignedSeq = null;
      } else {
        this.alignedSeq = alignedSeq.toCharArray();
      }

      this.cigar = cigar;
      this.read = Read.newBuilder()
          .setAlignedSequence(seq)
          .addAllAlignedQuality(qualityScores)
          .setAlignment(LinearAlignment.newBuilder()
              .setPosition(Position.newBuilder()
                  .setReferenceName(CHROMOSOME)
                  .setPosition(INITIAL_OFFSET + refSeqStart)
                  .build())
              .addAllCigar(cigarStringToUnits(cigar, refSeqs))
              .build())
          .build();
      this.readBases = buildReadBaseList(alignedSeq, alignedRef, qualityScores,
          CHROMOSOME, INITIAL_OFFSET);
    }

    /**
     * Constructs the expected read base list for comparison with the results from the Cigar class.
     *
     * @param alignedSeq The read sequence adjusted to be aligned with the reference
     * @param alignedRef The reference sequence adjusted to be aligned
     * @param quals The quality score for each read base
     * @param chromosome The chromosome where the alignment is
     * @param originalPos The position of the first read base on the chromosome
     * @return A constructed read base list
     */
    private static List<ReadBaseWithReference> buildReadBaseList(String alignedSeq,
        String alignedRef, List<Integer> quals,
        String chromosome, Integer originalPos) {

      if ((alignedSeq == null) || (alignedRef == null)) {
        return null;
      }

      if (alignedSeq.length() > alignedRef.length()) {
        throw new IllegalArgumentException("The alignedRef is shorter than the alignedSeq:"
            + alignedSeq.length() + " vs " + alignedRef.length());
      }

      int refPositionOffset = 0;
      int qualityOffset = 0;
      ImmutableList.Builder<ReadBaseWithReference> readBaseList = ImmutableList.builder();
      for (int i = 0; i < alignedSeq.length(); i++) {
        // we only have a ReadBase if there is a base here in *BOTH* the reference
        // and the read sequence.
        if ((!isBase(alignedSeq.charAt(i)))
            || (!isBase(alignedRef.charAt(i)))) {
          // if the reference has a base but the sequence does not
          if (alignedRef.charAt(i) != '*') {
            refPositionOffset++;
          }
          if (isBase(Character.toUpperCase(Character.toUpperCase(alignedSeq.charAt(i))))) {
            qualityOffset++;
          }
          continue;
        }

        ReadBaseWithReference sr = new ReadBaseWithReference(
            new ReadBaseQuality(
                alignedSeq.substring(i, i + 1),
                quals.get(qualityOffset)),
            alignedRef.substring(i, i + 1),
            Position.newBuilder()
            .setReferenceName(chromosome)
            .setPosition(originalPos + refPositionOffset)
            .build());
        qualityOffset++;
        refPositionOffset++;
        readBaseList.add(sr);
      }

      return readBaseList.build();
    }

    /**
     * For testing, it's easier to set up the tests accurately using strings for the cigars.
     *
     * @param s cigar string to be converted into CigarUnits.
     * @return list of CigarUnits.
     */
    private static List<CigarUnit> cigarStringToUnits(String s, List<String> refSeqs) {

      ImmutableList.Builder<CigarUnit> cigarUnits = ImmutableList.builder();

      // check for an empty cigar string
      if ("*".equals(s)) {
        return cigarUnits.build();
      }

      // index into the list of reference sequences
      int i = 0;
      Matcher m = CIGAR_CHUNK_PATTERN.matcher(s);
      while (m.find()) {
        cigarUnits.add(CigarUnit.newBuilder()
            .setOperation(CIGAR_UNIT_OPERATION.get(m.group().substring(m.group().length() - 1)))
            .setOperationLength(Integer.parseInt(m.group().substring(0, m.group().length() - 1)))
            .setReferenceSequence(!refSeqs.isEmpty() ? refSeqs.get(i) : "")
            .build());
        i++;
      }

      return cigarUnits.build();
    }

  }

  /* The test cases below are taken from the SAM file spec.
   * For the quality scores, we use different increasing numbers to validate
   * indexes into the quality list.
   */
  static final List<CigarTestCase> cases;

  static {
    ImmutableList.Builder<CigarTestCase> caseBuilder = ImmutableList.builder();
    // no cigar string
    caseBuilder.add(new CigarTestCase(
        "TTAGATAAAGGATACTG",
        ImmutableList.of(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26),
        "*",
        null,
        null,
        null,
        0));

    caseBuilder.add(new CigarTestCase(
        "TTAGATAAAGGATACTG",
        ImmutableList.of(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26),
        "8M2I4M1D3M",
        "AGCATGTTAGATAA**GATAGCTGTGCTAGTAGGCAGTCAGCGCCAT",
        "      TTAGATAAAGGATA*CTG",
        ImmutableList.of("TTAGATAA", "**", "GATA", "", "CTG"),
        6));

    caseBuilder.add(new CigarTestCase(
        "AAAAGATAAGGATA",
        ImmutableList.of(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23),
        "3S6M1P1I4M",
        "AGCATGTTAGATAA**GATAGCTGTGCTAGTAGGCAGTCAGCGCCAT",
        "     aaaAGATAA*GGATA",
        ImmutableList.of("", "AGATAA", "*", "*", "GATA"),
        8));

    caseBuilder.add(new CigarTestCase(
        "GCCTAAGCTAA",
        ImmutableList.of(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
        "5S6M",
        "AGCATGTTAGATAA**GATAGCTGTGCTAGTAGGCAGTCAGCGCCAT",
        "   gcctaAGCTAA",
        ImmutableList.of("", "AGATAA"),
        8));

    caseBuilder.add(new CigarTestCase(
        "ATAGCTTCAGC",
        ImmutableList.of(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
        "6M14N5M",
        "AGCATGTTAGATAA**GATAGCTGTGCTAGTAGGCAGTCAGCGCCAT",
        "                 ATAGCT..............TCAGC",
        ImmutableList.of("ATAGCT", "GTGCTAGTAGGCAG", "TCAGC"),
        15));

    caseBuilder.add(new CigarTestCase(
        "TAGGC",
        ImmutableList.of(10, 11, 12, 13, 14),
        "6H5M",
        "AGCATGTTAGATAA**GATAGCTGTGCTAGTAGGCAGTCAGCGCCAT",
        // The spec has ttagctTAGGC instead of TAGGC.  We're
        // dropping the ttagct prefix because it's been
        // pruned out before we get to toCigarOffset.
        "                              TAGGC",
        ImmutableList.of("", "TAGGC"),
        28));

    caseBuilder.add(new CigarTestCase(
        "CAGCGGCAT",
        ImmutableList.of(10, 11, 12, 13, 14, 15, 16, 17, 18),
        "9M",
        "AGCATGTTAGATAA**GATAGCTGTGCTAGTAGGCAGTCAGCGCCAT",
        "                                      CAGCGGCAT",
        ImmutableList.of("CAGCGCCAT"),
        36));

    cases = caseBuilder.build();
  }

  @Test
  public void testReadBases() throws IOException {
    for (CigarTestCase tc : cases) {
      assertEquals(tc.readBases, ReadFunctions.extractReadBases(tc.read));
    }
  }

  @Test
  public void testIsOnChromosome() {
    assertFalse(ReadFunctions.IS_ON_CHROMOSOME.apply(Read.newBuilder()
        .setAlignment(LinearAlignment.newBuilder()
            .setPosition(Position.newBuilder()
                .setReferenceName("chrZ")
                .build())
            .build())
        .build()));
    assertTrue(ReadFunctions.IS_ON_CHROMOSOME.apply(Read.newBuilder()
        .setAlignment(LinearAlignment.newBuilder()
            .setPosition(Position.newBuilder()
                .setReferenceName("1")
                .build())
            .build())
        .build()));
  }

  @Test
  public void testIsNotQCFailure() {
    assertFalse(ReadFunctions.IS_NOT_QC_FAILURE.apply(Read.newBuilder()
        .setFailedVendorQualityChecks(true)
        .build()));
    assertTrue(ReadFunctions.IS_NOT_QC_FAILURE.apply(Read.newBuilder()
        .setFailedVendorQualityChecks(false)
        .build()));
  }

  @Test
  public void testIsNotDuplicate() {
    assertFalse(ReadFunctions.IS_NOT_DUPLICATE.apply(Read.newBuilder()
        .setDuplicateFragment(true)
        .build()));
    assertTrue(ReadFunctions.IS_NOT_DUPLICATE.apply(Read.newBuilder()
        .setDuplicateFragment(false)
        .build()));
  }

  @Test
  public void testIsProperPlacement() {
    assertTrue(ReadFunctions.IS_PROPER_PLACEMENT.apply(Read.newBuilder()
        .setProperPlacement(true)
        .build()));
    assertFalse(ReadFunctions.IS_PROPER_PLACEMENT.apply(Read.newBuilder()
        .setProperPlacement(false)
        .build()));
  }

}
