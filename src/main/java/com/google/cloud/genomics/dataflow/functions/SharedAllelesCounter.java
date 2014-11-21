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
import java.util.logging.Logger;

import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.utils.CallFilters;
import com.google.cloud.dataflow.utils.PairGenerator;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * For each pair of calls on any of the given variants, computes the ratio, a number between 0 and 1
 * (inclusive), of the shared alleles between the two calls. Then, emits the sum and number of the
 * computed ratios for each pair of call set names across all the given variants.
 *
 * See <a href="http://konradjkarczewski.files.wordpress.com/2012/02/identity-howto.pdf">this
 * pdf</a> and <a
 * href="https://www.udacity.com/course/viewer#!/c-bio110/l-301726617/e-300477269/m-300477271"
 * >video</a> descriptions of Identity By State (IBS) for the basis of this implementation.
 */
public class SharedAllelesCounter extends
    DoFn<Variant, KV<KV<String, String>, KV<Double, Integer>>> {

  private static final Logger LOG = Logger.getLogger(SharedAllelesCounter.class
      .getName());

  private int numberOfVariants = 0;

  // HashMap<KV<callsetname1, callsetname2>, sum of ratios of shared alleles>
  private HashMap<KV<String, String>, Double> accumulator;

  @Override
  public void startBatch(Context c) {
    accumulator = Maps.newHashMap();
  }

  @Override
  public void processElement(ProcessContext context) {
    Variant variant = context.element();
    FluentIterable<KV<Call, Call>> pairs =
        PairGenerator.<Call, ImmutableList<Call>>allPairs(getSamplesWithVariant(variant));
    for (KV<Call, Call> pair : pairs) {
      accumulateSharedRatio(pair.getKey(), pair.getValue());
    }
    ++numberOfVariants;
  }

  private void accumulateSharedRatio(Call call1, Call call2) {
    KV<String, String> callPair = KV.of(call1.getCallSetName(), call2.getCallSetName());
    if (!accumulator.containsKey(callPair)) {
      accumulator.put(callPair, 0.0);
    }
    accumulator.put(callPair, accumulator.get(callPair) + ratioOfSharedAlleles(call1, call2));
  }

  @Override
  public void finishBatch(Context context) {
    for (KV<String, String> entry : accumulator.keySet()) {
      String call1 = entry.getKey();
      String call2 = entry.getValue();
      KV<String, String> call12 = KV.of(call1, call2);
      double sumOfRatios = accumulator.get(call12);
      int numberOfRatios = numberOfVariants;
      context.output(KV.of(call12, KV.of(sumOfRatios, numberOfRatios)));
      LOG.info("Emitted <" + call1 + ", " + call2 + ", " + sumOfRatios + ", " + numberOfRatios
          + "> for " + numberOfVariants + " variants.");
    }
  }

  static ImmutableList<Call> getSamplesWithVariant(Variant variant) {
    return CallFilters.getSamplesWithVariantOfMinGenotype(variant, 0);
  }

  // TODO: Double check that the following is the right way of computing the IBS
  // scores when the number of alleles is different than 2 and when the genotypes are unphased.
  static double ratioOfSharedAlleles(Call call1, Call call2) {
    int minNumberOfGenotypes = Math.min(call1.getGenotype().size(), call2.getGenotype().size());
    int numberOfSharedAlleles = 0;
    for (int i = 0; i < minNumberOfGenotypes; ++i) {
      if (call1.getGenotype().get(i) == call2.getGenotype().get(i)) {
        ++numberOfSharedAlleles;
      }
    }
    int maxNumberOfGenotypes = Math.max(call1.getGenotype().size(), call2.getGenotype().size());
    return (double) numberOfSharedAlleles / maxNumberOfGenotypes;
  }

}
