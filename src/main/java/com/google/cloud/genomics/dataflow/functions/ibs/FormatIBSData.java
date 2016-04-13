/*
 * Copyright 2014 Google Inc. All rights reserved.
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

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Joiner;

/**
 * Computes the IBS score based on the given sums of ratios and numbers of shared alleles. Then, it
 * outputs the results to a file.
 */
public final class FormatIBSData extends DoFn<KV<KV<String, String>, KV<Double, Integer>>, String> {
  @Override
  public void processElement(ProcessContext c) {
    KV<KV<String, String>, KV<Double, Integer>> result = c.element();
    String call1 = result.getKey().getKey();
    String call2 = result.getKey().getValue();
    Double sumOfRatios = result.getValue().getKey();
    int numberOfRatios = result.getValue().getValue();
    c.output(Joiner.on('\t').join(call1, call2, sumOfRatios / numberOfRatios, sumOfRatios,
        numberOfRatios));
  }
}
