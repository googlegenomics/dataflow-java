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
import com.google.cloud.dataflow.sdk.transforms.AsIterable;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.FromIterable;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SeqDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

/**
 * Given a set of similar pairs, this function collapses them into counts, runs PCA,
 * and writes the result to a tab-delimited GCS file.
*/
public class OutputPcaFile extends PTransform<PCollection<KV<String, String>>, PDone> {
  private final String outputFile;

  public OutputPcaFile(String outputFile) {
    this.outputFile = outputFile;
  }

  @Override
  public PDone apply(PCollection<KV<String, String>> similarPairs) {
    return similarPairs.apply(Count.<KV<String, String>>create())
        .apply(AsIterable.<KV<KV<String, String>, Long>>create())
        .apply(SeqDo.named("PCAAnalysis").of(new PcaAnalysis()))
        .apply(FromIterable.<PcaAnalysis.GraphResult>create())
        .apply(ParDo.named("FormatGraphData").of(new DoFn<PcaAnalysis.GraphResult, String>() {
          @Override
          public void processElement(ProcessContext c) {
            PcaAnalysis.GraphResult result = c.element();
            // Note: the extra tab is so this format places nicely with
            // Google Sheet's bubble chart
            c.output(result.name + "\t\t" + result.graphX + "\t" + result.graphY);
          }
        }))
        .apply(TextIO.Write.named("WriteCounts").to(outputFile));
  }
}
