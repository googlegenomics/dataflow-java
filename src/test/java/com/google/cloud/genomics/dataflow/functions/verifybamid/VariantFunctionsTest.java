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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.genomics.v1.Variant;

/**
 * Tests for the VariantUtils class.
 */
@RunWith(JUnit4.class)
public class VariantFunctionsTest {

  @Test
  public void testIsPassing() {
    // Only success case as stated by VCF 4.2 format.
    assertTrue(VariantFunctions.IS_PASSING.apply(Variant.newBuilder()
        .addFilter("PASS")
        .build()));
    // Typical failue case
    assertFalse(VariantFunctions.IS_PASSING.apply(Variant.newBuilder()
        .addFilter("BAD")
        .build()));
    // Failue with multiple annotations
    assertFalse(VariantFunctions.IS_PASSING.apply(Variant.newBuilder()
        .addFilter("BAD")
        .addFilter("q<10")
        .build()));
    // Malformed case; PASS with additional annotations
    assertFalse(VariantFunctions.IS_PASSING.apply(Variant.newBuilder()
        .addFilter("PASS")
        .addFilter("q<10")
        .build()));
  }

  @Test
  public void testIsOnChromosome() {
    assertFalse(VariantFunctions.IS_ON_CHROMOSOME.apply(Variant.newBuilder()
        .setReferenceName("chrZ")
        .build()));
    assertTrue(VariantFunctions.IS_ON_CHROMOSOME.apply(Variant.newBuilder()
        .setReferenceName("1")
        .build()));
  }

  @Test
  public void testIsNotLowQuality() {
    assertFalse(VariantFunctions.IS_NOT_LOW_QUALITY.apply(Variant.newBuilder()
        .setQuality(0.0)
        .build()));
    assertTrue(VariantFunctions.IS_NOT_LOW_QUALITY.apply(Variant.newBuilder()
        .setQuality(10.0)
        .build()));
  }

  @Test
  public void testIsSingleAlternateSnp() {
    // no alternates
    assertFalse(VariantFunctions.IS_SINGLE_ALTERNATE_SNP.apply(Variant.newBuilder()
        .setReferenceBases("A")
        .build()));
    // multiple alternates
    assertFalse(VariantFunctions.IS_SINGLE_ALTERNATE_SNP.apply(Variant.newBuilder()
        .setReferenceBases("A")
        .addAlternateBases("G")
        .addAlternateBases("T")
        .build()));
    // ref too long
    assertFalse(VariantFunctions.IS_SINGLE_ALTERNATE_SNP.apply(Variant.newBuilder()
        .setReferenceBases("AC")
        .addAlternateBases("G")
        .build()));
    // alternate too long
    assertFalse(VariantFunctions.IS_SINGLE_ALTERNATE_SNP.apply(Variant.newBuilder()
        .setReferenceBases("A")
        .addAlternateBases("GT")
        .build()));
    // success
    assertTrue(VariantFunctions.IS_SINGLE_ALTERNATE_SNP.apply(Variant.newBuilder()
        .setReferenceBases("A")
        .addAlternateBases("G")
        .build()));
  }

}