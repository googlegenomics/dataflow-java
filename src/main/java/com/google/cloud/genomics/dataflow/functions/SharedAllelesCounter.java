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

import java.util.logging.Logger;

import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.utils.CallFilters;
import com.google.cloud.dataflow.utils.PairGenerator;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;

/**
 * For each pair of calls on any of the given variants, computes the ratio, a number between 0 and 1
 * (inclusive), of the shared alleles between the two calls. Then, emits the sum and number of the
 * computed ratios for each pair of call set names across all the given variants.
 */
public class SharedAllelesCounter extends
    DoFn<Variant, KV<KV<String, String>, KV<Double, Integer>>> {

  private static final Logger LOG = Logger.getLogger(SharedAllelesCounter.class
      .getName());

  private int numberOfVariants = 0;

  private ImmutableMultimap.Builder<KV<String, String>, Double> accumulator;

  @Override
  public void startBatch(Context c) {
    accumulator = ImmutableMultimap.builder();
  }

  @Override
  public void processElement(ProcessContext context) {
    Variant variant = context.element();
    for (KV<Call, Call> pair : PairGenerator
        .<Call, ImmutableList<Call>>allPairs(getSamplesWithVariant(variant))) {
      Call call1 = pair.getKey();
      Call call2 = pair.getValue();
      accumulator.put(KV.of(call1.getCallSetName(), call2.getCallSetName()),
          ratioOfSharedAlleles(call1, call2));
    }
    ++numberOfVariants;
  }

  @Override
  public void finishBatch(Context context) {
    ImmutableMultimap<KV<String, String>, Double> acc = accumulator.build();
    for (KV<String, String> entry : acc.keySet()) {
      String call1 = entry.getKey();
      String call2 = entry.getValue();
      KV<String, String> call12 = KV.of(call1, call2);
      ImmutableCollection<Double> ratios = acc.get(call12);
      double sumOfRatios = sumDoubles(ratios);
      int numberOfRatios = ratios.size();
      context.output(KV.of(call12, KV.of(sumOfRatios, numberOfRatios)));
      LOG.info("Emitted " + call1 + ", " + call2 + ", " + sumOfRatios + ", " + numberOfRatios
          + " for " + numberOfVariants + " variants.");
    }
  }

  private double sumDoubles(ImmutableCollection<Double> call12ratios) {
    double sumOfRatios = 0;
    for (double ratio : call12ratios) {
      sumOfRatios += ratio;
    }
    return sumOfRatios;
  }

  static ImmutableList<Call> getSamplesWithVariant(Variant variant) {
    return CallFilters.getSamplesWithVariantOfMinGenotype(variant, 0);
  }

  // TODO(reprogrammer): Double check that the following is the right way of computing the IBS
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
