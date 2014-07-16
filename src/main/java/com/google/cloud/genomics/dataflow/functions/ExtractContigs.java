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
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.values.KV;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.logging.Logger;

/**
 * Given a path to a contigs.fasta file on GCS, pulls all contigs and emits them
 * 
 * Input: KV<Name, ContigPath>
 * Output: KV<Name, Contig>
 * 
 * Arguments: Thresholds for emitting contigs, Coverage and Length. Any contigs with shorter length
 * or lesser contigs than the specified thresholds will not be emitted. If arguments are missing
 * or set to -1 then there will be no filtering.
 */
public class ExtractContigs extends DoFn<KV<String, String>, KV<String, String>> {
  private static final Logger LOG = Logger.getLogger(ExtractContigs.class.getName());
  private final int lengthThreshold;
  private final double coverageThreshold;
  
  public ExtractContigs(int lengthThreshold, double coverageThreshold) {
    this.lengthThreshold = lengthThreshold;
    this.coverageThreshold = coverageThreshold;
  }
  
  @Override
  public void processElement(ProcessContext c) {
    KV<String, String> elem = c.element();
    String accession = elem.getKey();
    String contigPath = elem.getValue();
    GcsUtil gcsUtil = GcsUtil.create(c.getPipelineOptions());
   
    try {
      SeekableByteChannel contigChannel = gcsUtil.open(GcsUtil.asGcsFilename(contigPath));
      BufferedReader br = new BufferedReader(
          new InputStreamReader(Channels.newInputStream(contigChannel)));
      
      StringBuffer seq = new StringBuffer();
      boolean skipNext = false;
      String nextline;
      while ((nextline = br.readLine()) != null) {
        if (nextline.contains(">")) {
          // Output contig if meets criteria and clear string buffer
          if (!skipNext) {
            c.output(KV.of(accession, seq.toString()));
          }
          seq.delete(0, seq.length());
          
          // Determine stats for next contig
          String[] data = nextline.split("_");
          assert(data[2].equals("length"));
          assert(data[4].equals("cov"));
          int length = Integer.parseInt(data[3]);
          double coverage = Double.parseDouble(data[5]);
          
          if(length < lengthThreshold || coverage < coverageThreshold) {
            LOG.info("Contig ID " + data[7] + " for accession " + accession
                + " does not meet filter criteria, skipping");
            skipNext = true;
          } else {
            LOG.info("Including contig ID " + data[7] + " for accession " + accession);
            skipNext = false;
          }
        } else {
          seq.append(nextline);
        }
      }
    } catch (IOException e) {
      LOG.severe("Error reading contig file for " + accession);
      e.printStackTrace();
    }
  }
}
