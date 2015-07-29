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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.services.genomics.model.Call;
import com.google.genomics.v1.Variant;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for the VariantUtils class.
 */
@RunWith(JUnit4.class)
public class VariantUtilsTest {

  private List<String> emptyAlt = Arrays.asList();

  @Test
  public void testIsVariant() {
    // SNPs
    assertFalse(VariantUtils.IS_NON_VARIANT_SEGMENT.apply(DataUtils.makeVariant("chr7", 200000, 200001, "A", Arrays.asList("C"),
        (Call[]) null)));

    // Insertions
    assertFalse(VariantUtils.IS_NON_VARIANT_SEGMENT.apply(DataUtils.makeVariant("chr7", 200000, 200001, "A", Arrays.asList("AC"),
        (Call[]) null)));

    // Deletions NOTE: These are all the same mutation, just encoded in different ways.
    assertFalse(VariantUtils.IS_NON_VARIANT_SEGMENT.apply(DataUtils.makeVariant("chr7", 200000, 200001, "CAG", Arrays.asList("C"),
        (Call[]) null)));
    assertFalse(VariantUtils.IS_NON_VARIANT_SEGMENT.apply(DataUtils.makeVariant("chr7", 200000, 200001, "AG", emptyAlt,
        (Call[]) null)));
    assertFalse(VariantUtils.IS_NON_VARIANT_SEGMENT.apply(DataUtils.makeVariant("chr7", 200000, 200001, "AG", null,
        (Call[]) null)));

    // Multi-allelic sites
    assertFalse(VariantUtils.IS_NON_VARIANT_SEGMENT.apply(DataUtils.makeVariant("chr7", 200000, 200001, "A", Arrays.asList("C", "AC"),
        (Call[]) null)));
    assertFalse(VariantUtils.IS_NON_VARIANT_SEGMENT.apply(DataUtils.makeVariant("chr7", 200000, 200001, "A", Arrays.asList("C", "G"),
        (Call[]) null)));

    // Non-Variant Block Records
    assertTrue(VariantUtils.IS_NON_VARIANT_SEGMENT.apply(DataUtils.makeVariant("chr7", 200000, 200001, "A", emptyAlt,
        (Call[]) null)));
    assertTrue(VariantUtils.IS_NON_VARIANT_SEGMENT.apply(DataUtils.makeVariant("chr7", 200000, 200001, "A", null,
        (Call[]) null)));
    assertTrue(VariantUtils.IS_NON_VARIANT_SEGMENT.apply(DataUtils.makeVariant("chr7", 200000, 200001, "A", Arrays.asList(VariantUtils.GATK_NON_VARIANT_SEGMENT_ALT),
        (Call[]) null)));
  }

  @Test
  public void testIsSNP() {
    assertTrue(VariantUtils.IS_SNP.apply(DataUtils.makeVariant("chr7", 200000, 200001, "A",
        Arrays.asList("C"), (Call[]) null)));
    // Deletion
    assertFalse(VariantUtils.IS_SNP.apply(DataUtils.makeVariant("chr7", 200000, 200001, "CA",
        Arrays.asList("C"), (Call[]) null)));
    // Insertion
    assertFalse(VariantUtils.IS_SNP.apply(DataUtils.makeVariant("chr7", 200000, 200001, "C",
        Arrays.asList("CA"), (Call[]) null)));

    // SNP and Insertion
    assertFalse(VariantUtils.IS_SNP.apply(DataUtils.makeVariant("chr7", 200000, 200001, "C",
        Arrays.asList("A", "CA"), (Call[]) null)));

    // Block Records
    assertFalse(VariantUtils.IS_SNP.apply(DataUtils.makeVariant("chr7", 200000, 200001, "A",
        emptyAlt, (Call[]) null)));
    assertFalse(VariantUtils.IS_SNP.apply(DataUtils.makeVariant("chr7", 200000, 200001, "A", null,
        (Call[]) null)));
  }

  @Test
  public void testIsPassing() {
    // Only success case as stated by VCF 4.2 format.
    assertTrue(VariantUtils.IS_PASSING.apply(Variant.newBuilder()
        .addFilter("PASS")
        .build()));
    // Typical failue case
    assertFalse(VariantUtils.IS_PASSING.apply(Variant.newBuilder()
        .addFilter("BAD")
        .build()));
    // Failue with multiple annotations
    assertFalse(VariantUtils.IS_PASSING.apply(Variant.newBuilder()
        .addFilter("BAD")
        .addFilter("q<10")
        .build()));
    // Malformed case; PASS with additional annotations
    assertFalse(VariantUtils.IS_PASSING.apply(Variant.newBuilder()
        .addFilter("PASS")
        .addFilter("q<10")
        .build()));
  }

  @Test
  public void testIsOnChromosome() {
    assertFalse(VariantUtils.IS_ON_CHROMOSOME.apply(Variant.newBuilder()
        .setReferenceName("chrZ")
        .build()));
    assertTrue(VariantUtils.IS_ON_CHROMOSOME.apply(Variant.newBuilder()
        .setReferenceName("1")
        .build()));
  }

  @Test
  public void testIsNotLowQuality() {
    assertFalse(VariantUtils.IS_NOT_LOW_QUALITY.apply(Variant.newBuilder()
        .setQuality(0.0)
        .build()));
    assertTrue(VariantUtils.IS_NOT_LOW_QUALITY.apply(Variant.newBuilder()
        .setQuality(10.0)
        .build()));
  }

  @Test
  public void testIsSingleAlternateSnp() {
    // no alternates
    assertFalse(VariantUtils.IS_SINGLE_ALTERNATE_SNP.apply(Variant.newBuilder()
        .setReferenceBases("A")
        .build()));
    // multiple alternates
    assertFalse(VariantUtils.IS_SINGLE_ALTERNATE_SNP.apply(Variant.newBuilder()
        .setReferenceBases("A")
        .addAlternateBases("G")
        .addAlternateBases("T")
        .build()));
    // ref too long
    assertFalse(VariantUtils.IS_SINGLE_ALTERNATE_SNP.apply(Variant.newBuilder()
        .setReferenceBases("AC")
        .addAlternateBases("G")
        .build()));
    // alternate too long
    assertFalse(VariantUtils.IS_SINGLE_ALTERNATE_SNP.apply(Variant.newBuilder()
        .setReferenceBases("A")
        .addAlternateBases("GT")
        .build()));
    // success
    assertTrue(VariantUtils.IS_SINGLE_ALTERNATE_SNP.apply(Variant.newBuilder()
        .setReferenceBases("A")
        .addAlternateBases("G")
        .build()));
  }

}