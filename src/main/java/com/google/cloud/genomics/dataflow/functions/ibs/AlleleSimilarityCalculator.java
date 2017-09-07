/*
 * Copyright (C) 2014 Google Inc.
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
package com.google.cloud.genomics.dataflow.functions.ibs;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import com.google.cloud.genomics.dataflow.utils.CallFilters;
import com.google.cloud.genomics.dataflow.utils.PairGenerator;
import com.google.cloud.genomics.utils.grpc.VariantCallUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.genomics.v1.Variant;
import com.google.genomics.v1.VariantCall;

import java.util.HashMap;
import java.util.Map;

/**
 * For each pair of calls on any of the given variants, computes a score, a number between 0 and 1
 * (inclusive), which represents the degree of the similarity of the two calls. Then, it emits the
 * sum and number of the computed scores for each pair of call set names across all the given
 * variants.
 */
public class AlleleSimilarityCalculator extends
    DoFn<Variant, KV<KV<String, String>, KV<Double, Integer>>> {

  private CallSimilarityCalculatorFactory callSimilarityCalculatorFactory;
  private BoundedWindow window;

  // HashMap<KV<callsetname1, callsetname2>, <sum of call similarity scores, count of scores>
  private HashMap<KV<String, String>, KV<Double, Integer>> accumulator;
  // TODO consider using guava MultiSet instead

  public AlleleSimilarityCalculator(CallSimilarityCalculatorFactory callSimilarityCalculatorFactory) {
    this.callSimilarityCalculatorFactory = callSimilarityCalculatorFactory;
  }

  @StartBundle
  public void startBundle(StartBundleContext c) {
    accumulator = Maps.newHashMap();
  }

  @ProcessElement
  public void processElement(ProcessContext context, BoundedWindow window) {
    this.window = window;

    Variant variant = context.element();
    CallSimilarityCalculator callSimilarityCalculator =
        callSimilarityCalculatorFactory.get(isReferenceMajor(variant));
    for (KV<VariantCall, VariantCall> pair : PairGenerator.WITHOUT_REPLACEMENT.allPairs(
        getSamplesWithVariant(variant), VariantCallUtils.CALL_COMPARATOR)) {
      accumulateCallSimilarity(callSimilarityCalculator, pair.getKey(), pair.getValue());
    }
  }

  private void accumulateCallSimilarity(CallSimilarityCalculator callSimilarityCalculator,
      VariantCall call1, VariantCall call2) {
    KV<String, String> callPair = KV.of(call1.getCallSetName(), call2.getCallSetName());
    KV<Double, Integer> callPairAccumulation = accumulator.get(callPair);
    if (callPairAccumulation == null) {
      callPairAccumulation = KV.of(0.0, 0);
      accumulator.put(callPair, callPairAccumulation);
    }
    accumulator.put(callPair, KV.of(
        callPairAccumulation.getKey() + callSimilarityCalculator.similarity(call1, call2),
        callPairAccumulation.getValue() + 1));
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) {
    output(context, accumulator, window);
  }

  static ImmutableList<VariantCall> getSamplesWithVariant(Variant variant) {
    return CallFilters.getSamplesWithVariantOfMinGenotype(variant, 0);
  }

  static boolean isReferenceMajor(Variant variant) {
    int referenceAlleles = 0;
    int alternateAlleles = 0;
    for (VariantCall call : variant.getCallsList()) {
      for (Integer i : call.getGenotypeList()) {
        if (i == 0) {
          ++referenceAlleles;
        } else if (i > 0) {
          ++alternateAlleles;
        }
      }
    }
    return referenceAlleles >= alternateAlleles;
  }

  static <K, V> void output(DoFn<?, KV<K, V>>.FinishBundleContext context,
                            Map<? extends K, ? extends V> map,
                            BoundedWindow window) {
    for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
      context.output(KV.<K, V>of(entry.getKey(), entry.getValue()), window.maxTimestamp(), window);
    }
  }

}
