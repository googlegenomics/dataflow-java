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

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Generates kmer from a set of reads
 */
public class ReadsToKmers extends DoFn<KV<String, Read>, KV<String, String>> {
  private final int kValue;
  
  public ReadsToKmers(int kValue) {
    this.kValue = kValue;
  }
  
  @Override
  public void processElement(ProcessContext c) {
    String name = c.element().getKey();
    String seq = c.element().getValue().getOriginalBases();
    List<String> kmers = getKmers(seq);
    for (String kmer : kmers) {
      c.output(KV.of(name, kmer));
    }
  }
  
  @VisibleForTesting
  List<String> getKmers(String seq) {
    List<String> result = Lists.newArrayList();
    
    StringBuilder kmer = new StringBuilder(kValue);
    int skip = 0;
    for (int i = 0; i < seq.length(); i++) {
      kmer.append(seq.charAt(i));
      // Skip all k-mers with unknown nucleotides
      if (seq.charAt(i) == 'N') {
        skip = kValue;
      }
      
      if (kmer.length() == kValue) {
        if (skip == 0) {
          result.add(kmer.toString());
        }
        kmer.deleteCharAt(0);
      }
      skip = Math.max(0, skip - 1);
    }
    return result;
  }
}
