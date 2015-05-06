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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.api.services.genomics.model.Call;

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
    assertFalse(VariantUtils.IS_SNP.apply(DataUtils.makeVariant("chr7", 200000, 200001, "A", emptyAlt,
        (Call[]) null)));
    assertFalse(VariantUtils.IS_SNP.apply(DataUtils.makeVariant("chr7", 200000, 200001, "A", null,
        (Call[]) null)));
  }


}
