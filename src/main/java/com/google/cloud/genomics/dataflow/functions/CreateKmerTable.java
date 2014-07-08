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

import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

 
public class CreateKmerTable implements SerializableFunction<Iterable<KV<KV<String, String>, Long>>,
    Iterable<String>> {

  @Override
  public Iterable<String> apply(Iterable<KV<KV<String, String>, Long>> similarityData) {
    // Transform the data into a matrix
    LinkedHashSet<String> accessions = new LinkedHashSet<String>();
    HashSet<String> kmers = new HashSet<String>();
    HashMap<String, Long> counts = new HashMap<String, Long>();

    for (KV<KV<String, String>, Long> entry : similarityData) {
      String accession = entry.getKey().getKey();
      String kmer = entry.getKey().getValue();
      accessions.add(accession);
      kmers.add(kmer);
      counts.put(accession + " " + kmer, entry.getValue());
    }
    
    List<String> table = Lists.newArrayList();
    
    StringBuilder title = new StringBuilder();
    title.append("Accessions");
    for (String kmer : kmers) {
      title.append(",").append(kmer);
    }
    table.add(title.toString());
    
    for (String accession : accessions) {
      StringBuilder line = new StringBuilder();
      line.append(accession);
      for (String kmer : kmers) {
        Long count = counts.get(accession + " " + kmer);
        line.append(",").append((count == null) ? 0 : count);
      }
      table.add(line.toString());
    }

    return table;
  }
  
}