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
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.utils.CallFilters;
import com.google.cloud.genomics.dataflow.utils.PairGenerator;
import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.genomics.v1.Variant;
import com.google.genomics.v1.VariantCall;

/**
 * Emits a callset pair every time they share a variant.
 */
public class ExtractSimilarCallsets {

  abstract static class ExtractSimilarCallsetsBase<V, C> extends DoFn<V, KV<KV<String, String>, Long>> {

    private ImmutableMultiset.Builder<KV<String, String>> accumulator;

    @Override
    public void startBundle(Context c) {
      accumulator = ImmutableMultiset.builder();
    }

    @Override
    public void processElement(ProcessContext context) {
      FluentIterable<KV<String, String>> pairs = PairGenerator.WITH_REPLACEMENT.allPairs(
          getSamplesWithVariant(context.element()), Ordering.natural());
      for (KV<String, String> pair : pairs) {
        accumulator.add(pair);
      }
    }

    @Override
    public void finishBundle(Context context) {
      for (Multiset.Entry<KV<String, String>> entry : accumulator.build().entrySet()) {
        context.output(KV.of(entry.getElement(), Long.valueOf(entry.getCount())));
      }
    }

    protected abstract ImmutableList<String> getSamplesWithVariant(V variant);
  }

  public static class v1 extends ExtractSimilarCallsetsBase<Variant, VariantCall> {

    @Override
    protected ImmutableList<String> getSamplesWithVariant(Variant variant) {
      return ImmutableList.copyOf(Iterables.transform(
          CallFilters.getSamplesWithVariantOfMinGenotype(variant, 1), new Function<VariantCall, String>() {

            @Override
            public String apply(VariantCall call) {
              return call.getCallSetName();
            }

          }));
    }
  }
  
  @Deprecated  // Remove this when fully migrated to gRPC.
  public static class v1beta2 extends ExtractSimilarCallsetsBase<com.google.api.services.genomics.model.Variant, Call> {

    @Override
    protected ImmutableList<String> getSamplesWithVariant(com.google.api.services.genomics.model.Variant variant) {
      return ImmutableList.copyOf(Iterables.transform(
          CallFilters.getSamplesWithVariantOfMinGenotype(variant, 1), new Function<Call, String>() {

            @Override
            public String apply(Call call) {
              return call.getCallSetName();
            }

          }));
    }
  }
}
