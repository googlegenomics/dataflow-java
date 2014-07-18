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

import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

import java.util.logging.Logger;

/**
 * Given a set of kmers, counts the occurrences of every kmer and writes the resulting index
 * to file. If writeTable is true, then the format will be a csv table, otherwise it will be
 * in the format of a list of entries, each entry being <Name>-<Kmer>-<Count>:
 * 
 * Soft requirement: If writeTable is true outFile should have .csv extension, otherwise .txt
*/
public class WriteKmers extends PTransform<PCollection<KV<String, String>>, PDone> {
  private static final Logger LOG = Logger.getLogger(WriteKmers.class.getName());
  private final String outputFile;
  private final boolean writeTable;

  public WriteKmers(String outputFile, boolean writeTable) {
    this.outputFile = outputFile;
    this.writeTable = writeTable;
    
    if ((writeTable && !outputFile.endsWith(".csv")) || 
        (!writeTable && !outputFile.endsWith(".txt"))) {
      LOG.warning("Output file extension doesn't agree with writeTable value\n"
          + "File: " + outputFile + ", writeTable: " + writeTable);
    }
  }
  
  /**
   * Converts kmer count format to a string
   */
  private class FormatKmer extends DoFn<KV<KV<String, String>, Long>, String> {

    @Override
    public void processElement(ProcessContext c) {
      KV<KV<String, String>, Long> elem = c.element();
      String accession = elem.getKey().getKey();
      String kmer = elem.getKey().getValue();
      Long count = elem.getValue();
      c.output(accession + "-" + kmer + "-" + count + ":");
    }
  }

  @Override
  public PDone apply(PCollection<KV<String, String>> kmers) {
    LOG.info("Counting kmers...");
    PCollection<KV<KV<String, String>, Long>> counts = 
        kmers.apply(Count.<KV<String, String>>create());
    
    PCollection<String> output;
    
    if(writeTable) {
      LOG.info("Writing kmers to " + outputFile + " in table format");
      output = counts.apply(new CreateKmerTable());
    } else {
      LOG.info("Writing kmers to " + outputFile + " in list format");
      output = counts.apply(ParDo.named("Format output").of(new FormatKmer()));
    }
    
    return output.apply(TextIO.Write.named("Write kmers").to(outputFile));
  }
}
