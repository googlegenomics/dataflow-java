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
import com.google.cloud.dataflow.sdk.transforms.Convert;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SeqDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

/**
 * Given a set of similar pair counts, this function aggregates the counts,
 * runs Principal Coordinate Analysis, and writes the result to a tab-delimited GCS file which
 * can be imported into Google Spreadsheets and rendered with a bubble graph.
 *
 * The input data must be for a similarity matrix which will be symmetric. This is not
 * the same as Principal Component Analysis.
*/
public class OutputPCoAFile extends PTransform<PCollection<KV<KV<String, String>, Long>>, PDone> {
  private final String outputFile;

  public OutputPCoAFile(String outputFile) {
    this.outputFile = outputFile;
  }

  @Override
  public PDone apply(PCollection<KV<KV<String, String>, Long>> similarPairs) {
    return similarPairs.apply(Sum.<KV<String, String>>longsPerKey())
        .apply(Convert.<KV<KV<String, String>, Long>>toIterable())
        .apply(SeqDo.named("PCoAAnalysis").of(new PCoAnalysis()))
        .apply(Convert.<PCoAnalysis.GraphResult>fromIterable())
        .apply(ParDo.named("FormatGraphData").of(new DoFn<PCoAnalysis.GraphResult, String>() {
          @Override
          public void processElement(ProcessContext c) {
            PCoAnalysis.GraphResult result = c.element();
            // Note: the extra tab is so this format plays nicely with
            // Google Sheet's bubble chart
            c.output(result.name + "\t\t" + result.graphX + "\t" + result.graphY);
          }
        }))
        .apply(TextIO.Write.named("WriteCounts").to(outputFile));
  }
}
