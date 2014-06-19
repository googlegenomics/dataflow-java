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
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Emits a callset pair every time they share a variant.
 */
public class ExtractSimilarCallsets extends DoFn<Variant, KV<String, String>> {

  @Override
  public void processElement(ProcessContext c) {
    Variant variant = c.element();
    List<String> samplesWithVariant = Lists.newArrayList();
    for (Call call : variant.getCalls()) {
      String genotype = call.getInfo().get("GT").get(0); // TODO: Change to use real genotype field
      genotype = genotype.replaceAll("[\\\\|0]", "");
      if (!genotype.isEmpty()) {
        samplesWithVariant.add(call.getCallsetName());
      }
    }

    for (String s1 : samplesWithVariant) {
      for (String s2 : samplesWithVariant) {
        c.output(KV.of(s1, s2));
      }
    }
  }
}
