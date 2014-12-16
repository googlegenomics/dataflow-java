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
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * Computes the similarity of two calls based on whether they share a minor allele or not.
 */
public class SharedMinorAllelesCalculator implements CallSimilarityCalculator {

  private final boolean isReferenceMajor;

  public SharedMinorAllelesCalculator(boolean isReferenceMajor) {
    this.isReferenceMajor = isReferenceMajor;
  }

  private boolean hasMinorAllele(Call call) {
    return Iterables.any(call.getGenotype(), new Predicate<Integer>() {

      @Override
      public boolean apply(Integer genotype) {
        if (isReferenceMajor) {
          return genotype > 0;
        } else {
          return genotype == 0;
        }
      }

    });
  }

  @Override
  public double similarity(Call call1, Call call2) {
    if (call1.getCallSetName().equals(call2.getCallSetName())) {
      return 1.0;
    }
    if (hasMinorAllele(call1) && hasMinorAllele(call2)) {
      return 1.0;
    } else {
      return 0.0;
    }
  }

}
