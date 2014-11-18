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

import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;
import java.util.logging.Logger;

import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Iterators;
import com.google.common.collect.Range;

/**
 * TODO(reprogrammer): Update the following comment.
 * Emits a callset pair every time they share a variant.
 */
public class SharedAllelesCallsOfVariantCounter extends DoFn<Variant, KV<KV<String, String>, Double>> {

  private static final Logger LOG = Logger.getLogger(SharedAllelesCallsOfVariantCounter.class
      .getName());

  private ImmutableMultiset.Builder<KV<Call, Call>> accumulator;

  private int numberOfVariants = 0;

  // @Override
  // public void startBatch(Context c) {
  // accumulator = ImmutableMultiset.builder();
  // }

  @Override
  public void processElement(ProcessContext context) {
    Variant variant = context.element();
    for (KV<Call, Call> pair : SharedAllelesCallsOfVariantCounter
        .<Call, ImmutableList<Call>>allPairs(getSamplesWithVariant(variant))) {
      // accumulator.add(pair);
      Call call1 = pair.getKey();
      Call call2 = pair.getValue();
      context.output(KV.of(KV.of(call1.getCallSetName(), call2.getCallSetName()),
          ratioOfSharedAlleles(call1, call2)));
      LOG.info("Emitted " + call1.getCallSetName() + ", " + call2.getCallSetName() + ", "
          + ratioOfSharedAlleles(call1, call2) + "\n");
    }
    ++numberOfVariants;
    LOG.info("Generated pairs of calls for " + numberOfVariants + " variants.");
  }

  // @Override
  // public void finishBatch(Context context) {
  // for (Multiset.Entry<KV<Call, Call>> entry : accumulator.build().entrySet()) {
  // Call call1 = entry.getElement().getKey();
  // Call call2 = entry.getElement().getValue();
  // context.output(KV.of(KV.of(call1.getCallSetName(), call2.getCallSetName()),
  // ratioOfSharedAlleles(call1, call2)));
  // Log.info("Emitted " + call1.getCallSetName() + ", " + call2.getCallSetName() + ", "
  // + ratioOfSharedAlleles(call1, call2) + "\n");
  // }
  // }

  @VisibleForTesting
  static <X, L extends List<? extends X> & RandomAccess>
      FluentIterable<KV<X, X>> allPairs(final L list) {
    return FluentIterable
        .from(ContiguousSet.create(Range.closedOpen(0, list.size()), DiscreteDomain.integers()))
        .transformAndConcat(
            new Function<Integer, Iterable<KV<X, X>>>() {
              @Override public Iterable<KV<X, X>> apply(final Integer i) {
                return new Iterable<KV<X, X>>() {
                      @Override public Iterator<KV<X, X>> iterator() {
                        return Iterators.transform(list.listIterator(i),
                            new Function<X, KV<X, X>>() {

                              private final X key = list.get(i);

                              @Override public KV<X, X> apply(X value) {
                                return KV.of(key, value);
                              }
                            });
                      }
                    };
              }
            });
  }

  @VisibleForTesting
  static ImmutableList<Call> getSamplesWithVariant(Variant variant) {
    ImmutableList.Builder<Call> samplesWithVariant = ImmutableList.builder();
    for (Call call : variant.getCalls()) {
      for (int genotype : call.getGenotype()) {
        if (0 <= genotype) {
          // TODO(reprogrammer): Update this comment.
          // Use a greater than zero test since no-calls are -1 and we
          // don't want to count those.
          samplesWithVariant.add(call);
          break;
        }
      }
    }
    return samplesWithVariant.build();
  }

  // TODO(reprogrammer): Double check that the following right way of computing the IBS scores when
  // the number of alleles is different than 2.
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
