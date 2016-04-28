/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dataflow.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.api.services.genomics.model.Annotation;
import com.google.api.services.genomics.model.CodingSequence;
import com.google.api.services.genomics.model.Exon;
import com.google.api.services.genomics.model.Transcript;
import com.google.cloud.genomics.dataflow.utils.AnnotationUtils.VariantEffect;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import htsjdk.samtools.util.SequenceUtil;

@RunWith(JUnit4.class)
public class AnnotationUtilsTest {
  @Test
  public void testDetermineVariantTranscriptEffect_simpleShort() {
    Annotation transcript = new Annotation()
      .setReferenceName("1")
      .setStart(2L)
      .setEnd(9L)
      .setTranscript(new Transcript()
        .setCodingSequence(new CodingSequence().setStart(2L).setEnd(9L))
        .setExons(ImmutableList.of(
            new Exon().setStart(2L).setEnd(9L).setFrame(0))));

    assertEquals("GATTACA -> GCTTACA, codon is GAT -> GCT, AA is D -> A",
        VariantEffect.NONSYNONYMOUS_SNP,
        AnnotationUtils.determineVariantTranscriptEffect(3L, "C", transcript, "GATTACA"));
    assertEquals("ATGTGAA -> ATGTGGA, codon is TGA -> TGG, AA is STOP -> W",
        VariantEffect.STOP_LOSS,
        AnnotationUtils.determineVariantTranscriptEffect(7L, "G", transcript, "ATGTGAA"));
    assertEquals("CCCAAAT -> CCCTAAT, codon is AAA -> TAA, AA is K -> STOP",
        VariantEffect.STOP_GAIN,
        AnnotationUtils.determineVariantTranscriptEffect(5L, "T", transcript, "CCCAAAT"));
    assertEquals("GATTACA -> GACTACA, codon is GAT -> GAC, AA is D -> D",
        VariantEffect.SYNONYMOUS_SNP,
        AnnotationUtils.determineVariantTranscriptEffect(4L, "C", transcript, "GATTACA"));
    assertNull("variant does not intersect transcript",
        AnnotationUtils.determineVariantTranscriptEffect(123L, "C", transcript, "GATTACA"));
  }

  @Test
  public void testDetermineVariantTranscriptEffect_reverseStrand() {
    Annotation transcript = new Annotation()
      .setReferenceName("1")
      .setStart(2L)
      .setEnd(20L)
      .setReverseStrand(true)
      .setTranscript(new Transcript()
        .setCodingSequence(new CodingSequence().setStart(3L).setEnd(18L))
        .setExons(ImmutableList.of(
            new Exon().setStart(2L).setEnd(7L).setFrame(2),
            new Exon().setStart(10L).setEnd(20L).setFrame(1))
        ));

    String bases = SequenceUtil.reverseComplement(
        // First exon [10, 20)
        "AC" + // 5' UTR
        "ATG" + "ACG" + "GT" +
        // intron
        "CCC" +
        // Second exon [2, 7)
        "G" + "TAG" +
        "G"); // 3' UTR
    assertEquals("ATG -> ACG (reverse complement), AA is M -> T",
        VariantEffect.NONSYNONYMOUS_SNP,
        AnnotationUtils.determineVariantTranscriptEffect(16L, "G", transcript, bases));
    assertEquals("TAG -> CAG (reverse complement), AA is STOP -> Q",
        VariantEffect.STOP_LOSS,
        AnnotationUtils.determineVariantTranscriptEffect(5L, "G", transcript, bases));
    assertNull("mutates intron",
        AnnotationUtils.determineVariantTranscriptEffect(9L, "C", transcript, bases));
    assertNull("mutates 5' UTR",
        AnnotationUtils.determineVariantTranscriptEffect(19L, "C", transcript, bases));
  }

  @Test
  public void testDetermineVariantTranscriptEffect_noncoding() {
    Annotation transcript = new Annotation()
      .setReferenceName("1")
      .setStart(2L)
      .setEnd(9L)
      .setTranscript(new Transcript()
        .setExons(ImmutableList.of(new Exon().setStart(2L).setEnd(9L))));

    assertNull(AnnotationUtils.determineVariantTranscriptEffect(3L, "C", transcript, "GATTACA"));
    assertNull(AnnotationUtils.determineVariantTranscriptEffect(11L, "C", transcript, "GATTACA"));
  }

  @Test
  public void testDetermineVariantTranscriptEffect_frameless() {
    Annotation transcript = new Annotation()
      .setReferenceName("1")
      .setStart(2L)
      .setEnd(9L)
      .setTranscript(new Transcript()
        .setCodingSequence(new CodingSequence().setStart(2L).setEnd(9L))
        .setExons(ImmutableList.of(new Exon().setStart(2L).setEnd(9L))));

    assertNull(AnnotationUtils.determineVariantTranscriptEffect(3L, "C", transcript, "GATTACA"));
    assertNull(AnnotationUtils.determineVariantTranscriptEffect(11L, "C", transcript, "GATTACA"));
  }

  @Test
  public void testDetermineVariantTranscriptEffect_multiExon() {
    String bases = Strings.repeat("ACTTGGGTCA", 60);
    Annotation transcript = new Annotation()
      .setReferenceName("1")
      .setStart(100L)
      .setEnd(700L)
      .setTranscript(new Transcript()
        .setCodingSequence(new CodingSequence().setStart(250L).setEnd(580L))
        .setExons(ImmutableList.of(
            new Exon().setStart(100L).setEnd(180L),
            new Exon().setStart(200L).setEnd(300L).setFrame(2),
            new Exon().setStart(400L).setEnd(500L).setFrame(1),
            new Exon().setStart(550L).setEnd(600L).setFrame(0))
        ));

    assertNull("mutates noncoding exon",
        AnnotationUtils.determineVariantTranscriptEffect(150L, "C", transcript, bases));
    assertNull("mutates noncoding region of coding exon",
        AnnotationUtils.determineVariantTranscriptEffect(240L, "C", transcript, bases));
    assertNull("mutates intron",
        AnnotationUtils.determineVariantTranscriptEffect(350L, "C", transcript, bases));
    assertEquals("mutates first coding base, ACT -> TCT",
        VariantEffect.NONSYNONYMOUS_SNP,
        AnnotationUtils.determineVariantTranscriptEffect(250L, "T", transcript, bases));
    assertEquals("mutates middle exon, TGG -> TCG",
        VariantEffect.NONSYNONYMOUS_SNP,
        AnnotationUtils.determineVariantTranscriptEffect(454L, "C", transcript, bases));
  }
}
