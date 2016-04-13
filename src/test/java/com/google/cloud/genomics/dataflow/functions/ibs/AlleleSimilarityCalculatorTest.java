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
package com.google.cloud.genomics.dataflow.functions.ibs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.utils.DataUtils;
import com.google.genomics.v1.Variant;

public class AlleleSimilarityCalculatorTest {

  static final Variant snp1 = Variant.newBuilder()
      .setReferenceName("chr7")
      .setStart(200019)
      .setEnd(200020)
      .setReferenceBases("T")
      .addAlternateBases("G")
      .addCalls(DataUtils.makeVariantCall("het-alt sample", 1, 0))
      .addCalls(DataUtils.makeVariantCall("hom-alt sample", 1, 1))
      .addCalls(DataUtils.makeVariantCall("hom-ref sample", 0, 0))
      .addCalls(DataUtils.makeVariantCall("hom-nocall sample", -1, -1))
      .addCalls(DataUtils.makeVariantCall("ref-nocall sample", -1, 0))
      .build();

  static final Variant snp2 = Variant.newBuilder()
      .setReferenceName("chr7")
      .setStart(200020)
      .setEnd(200021)
      .setReferenceBases("C")
      .addAlternateBases("A")
      .addCalls(DataUtils.makeVariantCall("hom-alt sample", 1, 1))
      .addCalls(DataUtils.makeVariantCall("het-alt sample", 0, 1))
      .addCalls(DataUtils.makeVariantCall("ref-nocall sample", 0, -1))
      .build();

  @Test
  public void testIsReferenceMajor() {
    assertTrue(AlleleSimilarityCalculator.isReferenceMajor(snp1));
    assertFalse(AlleleSimilarityCalculator.isReferenceMajor(snp2));
  }

  @Test
  public void testGetSamplesWithVariant() {
    assertEquals(4, AlleleSimilarityCalculator.getSamplesWithVariant(snp1).size());
    assertEquals(3, AlleleSimilarityCalculator.getSamplesWithVariant(snp2).size());
  }

  @Test
  public void testSharedMinorAllSimilarityFn() {

    CallSimilarityCalculatorFactory fac = new SharedMinorAllelesCalculatorFactory();
    DoFnTester<Variant, KV<KV<String, String>, KV<Double, Integer>>> simFn =
        DoFnTester.of(new AlleleSimilarityCalculator(fac));

    List<KV<KV<String, String>, KV<Double, Integer>>> outputSnp1 = simFn.processBatch(snp1);
    assertEquals(6, outputSnp1.size());
    assertThat(outputSnp1, CoreMatchers.hasItems(
        KV.of(KV.of("het-alt sample", "ref-nocall sample"), KV.of(0.0, 1)),
        KV.of(KV.of("het-alt sample", "hom-ref sample"), KV.of(0.0, 1)),
        KV.of(KV.of("het-alt sample", "hom-alt sample"), KV.of(1.0, 1)),
        KV.of(KV.of("hom-ref sample", "ref-nocall sample"), KV.of(0.0, 1)),
        KV.of(KV.of("hom-alt sample", "ref-nocall sample"), KV.of(0.0, 1)),
        KV.of(KV.of("hom-alt sample", "hom-ref sample"), KV.of(0.0, 1))));

    List<KV<KV<String, String>, KV<Double, Integer>>> outputSnp2 = simFn.processBatch(snp2);
    assertEquals(3, outputSnp2.size());
    assertThat(
        outputSnp2,
        CoreMatchers.hasItems(KV.of(KV.of("het-alt sample", "hom-alt sample"), KV.of(0.0, 1)),
            KV.of(KV.of("het-alt sample", "ref-nocall sample"), KV.of(1.0, 1)),
            KV.of(KV.of("hom-alt sample", "ref-nocall sample"), KV.of(0.0, 1))));

    Variant[] input = new Variant[] {snp1, snp2};
    List<KV<KV<String, String>, KV<Double, Integer>>> outputBoth = simFn.processBatch(input);
    assertEquals(6, outputBoth.size());
    assertThat(outputBoth, CoreMatchers.hasItems(
        KV.of(KV.of("het-alt sample", "ref-nocall sample"), KV.of(1.0, 2)),
        KV.of(KV.of("het-alt sample", "hom-ref sample"), KV.of(0.0, 1)),
        KV.of(KV.of("het-alt sample", "hom-alt sample"), KV.of(1.0, 2)),
        KV.of(KV.of("hom-ref sample", "ref-nocall sample"), KV.of(0.0, 1)),
        KV.of(KV.of("hom-alt sample", "ref-nocall sample"), KV.of(0.0, 2)),
        KV.of(KV.of("hom-alt sample", "hom-ref sample"), KV.of(0.0, 1))));

  }
}
