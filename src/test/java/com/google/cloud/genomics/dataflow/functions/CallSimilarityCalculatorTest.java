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
package com.google.cloud.genomics.dataflow.functions;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.utils.DataUtils;

@RunWith(JUnit4.class)
public class CallSimilarityCalculatorTest {

  private static final double DELTA = 1e-6;
  
  private List<Call> calls = newArrayList();

  private List<Variant> variants = newArrayList();
  
  private static final String H1 = "H1";

  private static final String H2 = "H2";

  private static final String H3 = "H3";

  @Before
  public void setUp() {
    calls.add(DataUtils.makeCall(H1, 0, 0));
    calls.add(DataUtils.makeCall(H2, 1, 0));
    calls.add(DataUtils.makeCall(H3, 0, 1));
    calls.add(DataUtils.makeCall(H2, 1, 1));
    calls.add(DataUtils.makeCall(H3, 1, 1));
    calls.add(DataUtils.makeCall(H2, 1));
    calls.add(DataUtils.makeCall(H3, 0));
    calls.add(DataUtils.makeCall(H2, 1, 0, 1));
    calls.add(DataUtils.makeCall(H3, 1, 0, 0));

    variants.add(DataUtils.makeSimpleVariant(calls.get(0), calls.get(1), calls.get(2)));
    variants.add(DataUtils.makeSimpleVariant(calls.get(0), calls.get(3), calls.get(4)));
    variants.add(DataUtils.makeSimpleVariant(calls.get(0), calls.get(5), calls.get(6)));
    variants.add(DataUtils.makeSimpleVariant(calls.get(0), calls.get(7), calls.get(8)));
  }

  @Test
  public void testSharedAllelesRatioCalculator() {
    CallSimilarityCalculator calculator = new SharedAllelesRatioCalculator();

    assertEquals(0.5, calculator.similarity(calls.get(0), calls.get(1)), DELTA);
    assertEquals(0, calculator.similarity(calls.get(0), calls.get(4)), DELTA);
    assertEquals(0.5, calculator.similarity(calls.get(3), calls.get(2)), DELTA);
    assertEquals(1, calculator.similarity(calls.get(3), calls.get(4)), DELTA);
    assertEquals(0.5, calculator.similarity(calls.get(4), calls.get(5)), DELTA);
    assertEquals(1.0 / 3.0, calculator.similarity(calls.get(5), calls.get(7)), DELTA);
    assertEquals(2.0 / 3.0, calculator.similarity(calls.get(7), calls.get(8)), DELTA);
    assertEquals(0, calculator.similarity(calls.get(6), calls.get(7)), DELTA);
  }

  @Test
  public void testIsReferenceMajor() {
    assertTrue(AlleleSimilarityCalculator.isReferenceMajor(variants.get(0)));
    assertFalse(AlleleSimilarityCalculator.isReferenceMajor(variants.get(1)));
    assertTrue(AlleleSimilarityCalculator.isReferenceMajor(variants.get(2)));
    assertTrue(AlleleSimilarityCalculator.isReferenceMajor(variants.get(3)));
  }
  
  @Test
  public void testSharedMinorAllelesCalculatorWhenReferenceIsMinor() {
    CallSimilarityCalculator calculator = new SharedMinorAllelesCalculator(false);
    
    assertEquals(1, calculator.similarity(calls.get(0), calls.get(1)), DELTA);
    assertEquals(0, calculator.similarity(calls.get(0), calls.get(4)), DELTA);
    assertEquals(0, calculator.similarity(calls.get(3), calls.get(2)), DELTA);
    assertEquals(0, calculator.similarity(calls.get(3), calls.get(4)), DELTA);
    assertEquals(0, calculator.similarity(calls.get(4), calls.get(5)), DELTA);
    assertEquals(1, calculator.similarity(calls.get(5), calls.get(7)), DELTA);
    assertEquals(1, calculator.similarity(calls.get(7), calls.get(8)), DELTA);
    assertEquals(1, calculator.similarity(calls.get(6), calls.get(7)), DELTA);
  }

  @Test
  public void testSharedMinorAllelesCalculatorWhenReferenceIsMajor() {
    CallSimilarityCalculator calculator = new SharedMinorAllelesCalculator(true);

    assertEquals(0, calculator.similarity(calls.get(0), calls.get(1)), DELTA);
    assertEquals(0, calculator.similarity(calls.get(0), calls.get(4)), DELTA);
    assertEquals(1, calculator.similarity(calls.get(3), calls.get(2)), DELTA);
    assertEquals(1, calculator.similarity(calls.get(3), calls.get(4)), DELTA);
    assertEquals(1, calculator.similarity(calls.get(4), calls.get(5)), DELTA);
    assertEquals(1, calculator.similarity(calls.get(5), calls.get(7)), DELTA);
    assertEquals(1, calculator.similarity(calls.get(7), calls.get(8)), DELTA);
    assertEquals(0, calculator.similarity(calls.get(6), calls.get(7)), DELTA);
  }

  @Test
  public void testAlleleSimilarityCalculatorWithSharedAllelesRatio() {
    Map<KV<String, String>, KV<Double, Integer>> fnOutputMap =
        calculatorOutputAsMap(new SharedAllelesRatioCalculatorFactory());

    assertEquals(fnOutputMap.get(KV.of(H1, H2)).getKey(), 0.5 + 1.0 / 3.0, DELTA);
    assertEquals(fnOutputMap.get(KV.of(H1, H3)).getKey(), 1.0 + 1.0 / 3.0, DELTA);
    assertEquals(fnOutputMap.get(KV.of(H2, H3)).getKey(), 1.0 + 2.0 / 3.0, DELTA);

    assertEquals(4, fnOutputMap.get(KV.of(H1, H2)).getValue().intValue());
    assertEquals(4, fnOutputMap.get(KV.of(H1, H3)).getValue().intValue());
    assertEquals(4, fnOutputMap.get(KV.of(H2, H3)).getValue().intValue());
  }

  @Test
  public void testAlleleSimilarityCalculatorWithSharedMinorAlleles() {
    Map<KV<String, String>, KV<Double, Integer>> fnOutputMap =
        calculatorOutputAsMap(new SharedMinorAllelesCalculatorFactory());

    assertEquals(0.0, fnOutputMap.get(KV.of(H1, H2)).getKey(), DELTA);
    assertEquals(0.0, fnOutputMap.get(KV.of(H1, H3)).getKey(), DELTA);
    assertEquals(2.0, fnOutputMap.get(KV.of(H2, H3)).getKey(), DELTA);

    assertEquals(4, fnOutputMap.get(KV.of(H1, H2)).getValue().intValue());
    assertEquals(4, fnOutputMap.get(KV.of(H1, H3)).getValue().intValue());
    assertEquals(4, fnOutputMap.get(KV.of(H2, H3)).getValue().intValue());
  }

  private Map<KV<String, String>, KV<Double, Integer>> calculatorOutputAsMap(
      CallSimilarityCalculatorFactory calculatorFactory) {
    DoFnTester<Variant, KV<KV<String, String>, KV<Double, Integer>>> fnTester =
        DoFnTester.of(new AlleleSimilarityCalculator(calculatorFactory));
    List<KV<KV<String, String>, KV<Double, Integer>>> fnOutput =
        fnTester.processBatch(variants.toArray(new Variant[] {}));
    Map<KV<String, String>, KV<Double, Integer>> fnOutputMap = newHashMap();
    for (KV<KV<String, String>, KV<Double, Integer>> kv : fnOutput) {
      fnOutputMap.put(kv.getKey(), kv.getValue());
    }
    return fnOutputMap;
  }

}
