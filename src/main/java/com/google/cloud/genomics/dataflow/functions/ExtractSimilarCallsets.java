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
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Emits a callset pair every time they share a variant.
 */
public class ExtractSimilarCallsets extends DoFn<Variant, KV<KV<String, String>, Long>> {

  private HashMap<KV<String, String>, Long> accumulator;

  @Override
  public void startBatch(Context c) {
    accumulator = new HashMap<KV<String, String>, Long>();
  }

  @Override
  public void processElement(ProcessContext c) {
    Variant variant = c.element();
    List<String> samples = getSamplesWithVariant(variant);

    // TODO: optimize this to emit all combinations instead of all permutations
    for (String s1 : samples) {
      for (String s2 : samples) {
        KV<String, String> key = KV.of(s1, s2);
        Long count = accumulator.get(key);
        if (null == count) {
          count = new Long(1);
        } else {
          count++;
        }
        accumulator.put(key, count);
      }
    }
  }

  @VisibleForTesting
  List<String> getSamplesWithVariant(Variant variant) {
    List<String> samplesWithVariant = Lists.newArrayList();
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
    return samplesWithVariant;
  }

  @Override
  public void finishBatch(Context c) {
    for (Map.Entry<KV<String, String>, Long> entry : accumulator.entrySet()) {
      c.output(KV.of(entry.getKey(), entry.getValue()));
    }
  }
}