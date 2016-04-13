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
package com.google.cloud.genomics.dataflow.functions.ibs;

import com.google.genomics.v1.VariantCall;

/**
 * See <a href="http://konradjkarczewski.files.wordpress.com/2012/02/identity-howto.pdf">this
 * pdf</a> and <a href="https://www.youtube.com/watch?v=NRiI1RbE_-I" >video</a> descriptions of
 * Identity By State (IBS) for the basis of this implementation.
 */
public class SharedAllelesRatioCalculator implements CallSimilarityCalculator {
  
  // TODO: Double check that the following is the right way of computing the IBS
  // scores when the number of alleles is different than 2 and when the genotypes are unphased.
  @Override
  public double similarity(VariantCall call1, VariantCall call2) {
    int minNumberOfGenotypes = Math.min(call1.getGenotypeCount(), call2.getGenotypeCount());
    int numberOfSharedAlleles = 0;
    for (int i = 0; i < minNumberOfGenotypes; ++i) {
      if (call1.getGenotype(i) == call2.getGenotype(i)) {
        ++numberOfSharedAlleles;
      }
    }
    int maxNumberOfGenotypes = Math.max(call1.getGenotypeCount(), call2.getGenotypeCount());
    return (double) numberOfSharedAlleles / maxNumberOfGenotypes;
  }


}
