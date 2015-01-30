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
package com.google.cloud.genomics.dataflow.functions;

import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.utils.CallFilters;
import com.google.cloud.genomics.dataflow.utils.PairGenerator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;

/**
 * Emits a callset pair every time they share a variant.
 */
public class ExtractSimilarCallsets extends DoFn<Variant, KV<KV<String, String>, Long>> {

  private ImmutableMultiset.Builder<KV<String, String>> accumulator;

  @Override
  public void startBundle(Context c) {
    accumulator = ImmutableMultiset.builder();
  }

  @Override
  public void processElement(ProcessContext context) {
    for (KV<String, String> pair : PairGenerator.WITH_REPLACEMENT.allPairs(
        getSamplesWithVariant(context.element()), String.CASE_INSENSITIVE_ORDER)) {
      accumulator.add(pair);
    }
  }

  @Override
  public void finishBundle(Context context) {
    for (Multiset.Entry<KV<String, String>> entry : accumulator.build().entrySet()) {
      context.output(KV.of(entry.getElement(), Long.valueOf(entry.getCount())));
    }
  }

  @VisibleForTesting
  static ImmutableList<String> getSamplesWithVariant(Variant variant) {
    return ImmutableList.copyOf(Iterables.transform(
        CallFilters.getSamplesWithVariantOfMinGenotype(variant, 1), new Function<Call, String>() {

          @Override
          public String apply(Call call) {
            return call.getCallSetName();
          }

        }));
  }
}
