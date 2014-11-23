/*
Copyright 2014 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.google.cloud.genomics.dataflow.functions;

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;

/**
 * Sums up a number of pairs of a double and an int to a single pair.
 *
 * Each pair represents the sum of ratio and number of shared alleles between two call set names for
 * a set of variants.
 */
public final class IBSCalculator implements
    SerializableFunction<Iterable<KV<Double, Integer>>, KV<Double, Integer>> {

  @Override
  public KV<Double, Integer> apply(Iterable<KV<Double, Integer>> sharedAllelesInfos) {
    double sumOfRatios = 0.0;
    int numberOfRatios = 0;
    for (KV<Double, Integer> info : sharedAllelesInfos) {
      sumOfRatios += info.getKey();
      numberOfRatios += info.getValue();
    }
    return KV.of(sumOfRatios, numberOfRatios);
  }

}