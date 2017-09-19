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
package com.google.cloud.genomics.dataflow.functions.pca;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import com.google.cloud.genomics.dataflow.utils.CallFilters;
import com.google.cloud.genomics.dataflow.utils.PairGenerator;
import com.google.common.base.Function;
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
public class ExtractSimilarCallsets extends DoFn<Variant, KV<KV<String, String>, Long>> {

  private ImmutableMultiset.Builder<KV<String, String>> accumulator;
  private BoundedWindow window;

  @StartBundle
  public void startBundle(StartBundleContext c) {
    accumulator = ImmutableMultiset.builder();
  }

  @ProcessElement
  public void processElement(ProcessContext context, BoundedWindow window) {
    this.window = window;
    FluentIterable<KV<String, String>> pairs = PairGenerator.WITH_REPLACEMENT.allPairs(
        getSamplesWithVariant(context.element()), Ordering.natural());
    for (KV<String, String> pair : pairs) {
      accumulator.add(pair);
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) {
    for (Multiset.Entry<KV<String, String>> entry : accumulator.build().entrySet()) {
      context.output(KV.of(entry.getElement(), Long.valueOf(entry.getCount())),
          window.maxTimestamp(), window);
    }
  }

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
