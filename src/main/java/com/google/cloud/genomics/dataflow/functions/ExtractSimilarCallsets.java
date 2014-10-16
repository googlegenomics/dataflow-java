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

import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ranges;

import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;

/**
 * Emits a callset pair every time they share a variant.
 */
public class ExtractSimilarCallsets extends DoFn<Variant, KV<KV<String, String>, Long>> {

  private ImmutableMultiset.Builder<KV<String, String>> accumulator;

  @Override
  public void startBatch(Context c) {
    accumulator = ImmutableMultiset.builder();
  }

  @Override
  public void processElement(ProcessContext context) {
    for (KV<String, String> pair : allPairs(getSamplesWithVariant(context.element()))) {
      accumulator.add(pair);
    }
  }

  @Override
  public void finishBatch(Context context) {
    for (Multiset.Entry<KV<String, String>> entry : accumulator.build().entrySet()) {
      context.output(KV.of(entry.getElement(), Long.valueOf(entry.getCount())));
    }
  }

  @VisibleForTesting
  static <X, L extends List<? extends X> & RandomAccess>
      FluentIterable<KV<X, X>> allPairs(final L list) {
    return FluentIterable
        .from(ContiguousSet.create(Ranges.closedOpen(0, list.size()), DiscreteDomains.integers()))
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
  static ImmutableList<String> getSamplesWithVariant(Variant variant) {
    ImmutableList.Builder<String> samplesWithVariant = ImmutableList.builder();
    for (Call call : variant.getCalls()) {
      for (int genotype : call.getGenotype()) {
        if (0 < genotype) {
          // Use a greater than zero test since no-calls are -1 and we
          // don't want to count those.
          samplesWithVariant.add(call.getCallSetName());
          break;
        }
      }
    }
    return samplesWithVariant.build();
  }
}