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
    assertTrue(VariantUtils.isVariant(DataUtils.makeVariant("chr7", 200000, 200001, "A",
        Arrays.asList("C"), (Call[]) null)));
    // Block Records
    assertFalse(VariantUtils.isVariant(DataUtils.makeVariant("chr7", 200000, 200001, "A", emptyAlt,
        (Call[]) null)));
    assertFalse(VariantUtils.isVariant(DataUtils.makeVariant("chr7", 200000, 200001, "A", null,
        (Call[]) null)));
  }

  @Test
  public void testIsSNP() {
    assertTrue(VariantUtils.isSnp(DataUtils.makeVariant("chr7", 200000, 200001, "A",
        Arrays.asList("C"), (Call[]) null)));
    // Deletion
    assertFalse(VariantUtils.isSnp(DataUtils.makeVariant("chr7", 200000, 200001, "CA",
        Arrays.asList("C"), (Call[]) null)));
    // Insertion
    assertFalse(VariantUtils.isSnp(DataUtils.makeVariant("chr7", 200000, 200001, "C",
        Arrays.asList("CA"), (Call[]) null)));

    // SNP and Insertion
    assertFalse(VariantUtils.isSnp(DataUtils.makeVariant("chr7", 200000, 200001, "C",
        Arrays.asList("A", "CA"), (Call[]) null)));

    // Block Records
    assertFalse(VariantUtils.isSnp(DataUtils.makeVariant("chr7", 200000, 200001, "A", emptyAlt,
        (Call[]) null)));
    assertFalse(VariantUtils.isSnp(DataUtils.makeVariant("chr7", 200000, 200001, "A", null,
        (Call[]) null)));
  }


}
