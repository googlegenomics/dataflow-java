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

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.data_structures.Allele;

// Calculates TransmissionProbability for each variant, based on the information
// that which parent is the source of each variant.

public class CalculateTransmissionDisequilibrium
    extends DoFn<KV<Allele, Iterable<Boolean>>, KV<Allele, Double>> {

  @Override
  public void processElement(ProcessContext c) {
    KV<Allele, Iterable<Boolean>> input = c.element();
    double transmitted = 0, total = 0;
    for (Boolean b : input.getValue()) {
      if (b) {
        transmitted += 1;
      }
      total += 1;
    }
    double tp = transmitted / total;
    c.output(KV.of(input.getKey(), tp));
  }
}
