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
package com.google.cloud.dataflow.utils;

import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.common.collect.ImmutableList;

public class CallFilters {

  public static ImmutableList<Call> getSamplesWithVariantOfMinGenotype(Variant variant,
      int minGenotype) {
    ImmutableList.Builder<Call> samplesWithVariant = ImmutableList.builder();
    for (Call call : variant.getCalls()) {
      for (int genotype : call.getGenotype()) {
        if (minGenotype <= genotype) {
          // Use a greater than zero test since no-calls are -1 and we
          // don't want to count those.
          samplesWithVariant.add(call);
          break;
        }
      }
    }
    return samplesWithVariant.build();
  }

}
