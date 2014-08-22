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

import java.util.List;
import java.util.logging.Logger;

/**
 * Emits a callset pair every time they share a variant.
 */
public class ExtractSimilarCallsets extends DoFn<Variant, KV<String, String>> {
  private static final Logger LOG = Logger.getLogger(ExtractSimilarCallsets.class.getName());

  @Override
  public void processElement(ProcessContext c) {
    Variant variant = c.element();
    List<String> samples = getSamplesWithVariant(variant);

    for (String s1 : samples) {
      for (String s2 : samples) {
        c.output(KV.of(s1, s2));
      }
    }
  }

  @VisibleForTesting
  List<String> getSamplesWithVariant(Variant variant) {
    List<String> samplesWithVariant = Lists.newArrayList();
    for (Call call : variant.getCalls()) {
      for (int genotype : call.getGenotype()) {
        if (0 < genotype) {
          // Use a great than zero test since no-calls are -1 and we
          // don't want to count those
          samplesWithVariant.add(call.getCallsetName());
          break;
        }
      }
    }
    return samplesWithVariant;
  }
}
