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

import java.util.HashMap;

import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.utils.CallFilters;
import com.google.cloud.genomics.dataflow.utils.PairGenerator;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * For each pair of calls on any of the given variants, computes a score, a number between 0 and 1
 * (inclusive), which represents the degree of the similarity of the two calls. Then, it emits the
 * sum and number of the computed scores for each pair of call set names across all the given
 * variants.
 */
public class AlleleSimilarityCalculator extends
    DoFn<Variant, KV<KV<String, String>, KV<Double, Integer>>> {

  private CallSimilarityCalculatorFactory callSimilarityCalculatorFactory;
  
  private int numberOfVariants = 0;

  // HashMap<KV<callsetname1, callsetname2>, sum of call similarity scores>
  private HashMap<KV<String, String>, Double> accumulator;

  public AlleleSimilarityCalculator(CallSimilarityCalculatorFactory callSimilarityCalculatorFactory) {
    this.callSimilarityCalculatorFactory = callSimilarityCalculatorFactory;
  }

  @Override
  public void startBatch(Context c) {
    accumulator = Maps.newHashMap();
  }

  @Override
  public void processElement(ProcessContext context) {
    Variant variant = context.element();
    CallSimilarityCalculator callSimilarityCalculator =
        callSimilarityCalculatorFactory.get(isReferenceMajor(variant));
    FluentIterable<KV<Call, Call>> pairs =
        PairGenerator.<Call, ImmutableList<Call>>allPairs(getSamplesWithVariant(variant));
    for (KV<Call, Call> pair : pairs) {
      accumulateCallSimilarity(callSimilarityCalculator, pair.getKey(), pair.getValue());
    }
    ++numberOfVariants;
  }

  private void accumulateCallSimilarity(CallSimilarityCalculator callSimilarityCalculator,
      Call call1, Call call2) {
    KV<String, String> callPair = KV.of(call1.getCallSetName(), call2.getCallSetName());
    if (!accumulator.containsKey(callPair)) {
      accumulator.put(callPair, 0.0);
    }
    accumulator.put(callPair,
        accumulator.get(callPair) + callSimilarityCalculator.similarity(call1, call2));
  }

  @Override
  public void finishBatch(Context context) {
    for (KV<String, String> entry : accumulator.keySet()) {
      String call1 = entry.getKey();
      String call2 = entry.getValue();
      KV<String, String> call12 = KV.of(call1, call2);
      double sumOfRatios = accumulator.get(call12);
      context.output(KV.of(call12, KV.of(sumOfRatios, numberOfVariants)));
    }
  }

  static ImmutableList<Call> getSamplesWithVariant(Variant variant) {
    return CallFilters.getSamplesWithVariantOfMinGenotype(variant, 0);
  }
  
  static boolean isReferenceMajor(Variant variant) {
    int referenceAlleles = 0;
    int alternateAlleles = 0;
    for (Call call : variant.getCalls()) {
      for (Integer i : call.getGenotype()) {
        if (i == 0) {
          ++referenceAlleles;
        } else {
          ++alternateAlleles;
        }
      }
    }
    return referenceAlleles >= alternateAlleles;
  }

}
