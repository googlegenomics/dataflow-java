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
package com.google.cloud.genomics.dataflow.pipelines;

import com.google.api.services.genomics.model.Readset;
import com.google.api.services.genomics.model.SearchReadsetsRequest;
import com.google.api.services.genomics.model.SearchReadsetsResponse;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.runners.Description;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.utils.OptionsParser;
import com.google.cloud.dataflow.utils.RequiredOption;
import com.google.cloud.genomics.dataflow.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.GenomicsApi;
import com.google.cloud.genomics.dataflow.GenomicsOptions;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.functions.ReadsToKmers;
import com.google.cloud.genomics.dataflow.functions.ReadsetToReads;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * Dataflows pipeline for performing PCA on generated kmer indicies from the reads in a dataset
 */
public class FDAPipeline {
  private static final Logger LOG = Logger.getLogger(FDAPipeline.class.getName());
  private static final String READSET_FIELDS = "nextPageToken,readsets(id,name)";
  private static final String READ_FIELDS = "nextPageToken,reads(originalBases)";

  // Do not instantiate
  private FDAPipeline() { }

  private static class Options extends GenomicsOptions {
    @Description("Path of the file to write PCA results to")
    @RequiredOption
    public String pcaOutput;
    
    @Description("Path of file to write Kmer indices to")
    public String kmerOutput;
    
    @Description("Numerical value of K for indexing. Should not be larger than ~100")
    @RequiredOption
    public Integer kValue;
    
    public void checkArgs() {
      if (kValue < 1 || kValue > 256) {
        throw new IllegalArgumentException("K value is out of bounds (must be between 1 and 256");
      }
    }
  }

  private static List<Readset> getReadsets(
      String accessToken, String apiKey, String datasetId) throws IOException {
    GenomicsApi api = new GenomicsApi(accessToken, apiKey);
    SearchReadsetsRequest request = new SearchReadsetsRequest()
        .setDatasetIds(Lists.newArrayList(datasetId));
    List<Readset> readsets = Lists.newArrayList();
    boolean done = false;
    while (!done) {
      SearchReadsetsResponse response = api.executeRequest(
          api.getService().readsets().search(request), READSET_FIELDS);
      readsets.addAll(response.getReadsets());
      if (response.getNextPageToken() != null) {
        request.setPageToken(response.getNextPageToken());
      } else {
        done = true;
      }
    }
    return readsets;
  }
  
  public static void main(String[] args) throws IOException {
    Options options = OptionsParser.parse(args, Options.class, FDAPipeline.class.getSimpleName());
    options.checkArgs();

    LOG.info("Starting pipeline...");
    String token = options.getAccessToken();
    List<Readset> readsets = getReadsets(token, options.apiKey, options.datasetId);
    
    Pipeline p = Pipeline.create();

    PCollection<KV<String, String>> kmers = DataflowWorkarounds.getPCollection(
        readsets, GenericJsonCoder.of(Readset.class), p, options.numWorkers)
        .apply(ParDo.named("Readsets To Reads")
            .of(new ReadsetToReads(token, options.apiKey, READ_FIELDS)))
        .apply(ParDo.named("Generate Kmers").of(new ReadsToKmers(options.kValue)));
    
    // Print to file
    if (options.kmerOutput != null) {
      kmers.apply(Count.<KV<String, String>>create())
          .apply(ParDo.named("Format Kmers").of(new DoFn<KV<KV<String, String>, Long>, String>() {

            @Override
            public void processElement(ProcessContext c) {
              KV<KV<String, String>, Long> elem = c.element();
              String name = elem.getKey().getKey();
              String kmer = elem.getKey().getValue();
              Long count = elem.getValue();
              c.output(name + "-" + kmer + "-" + count + ":");
            }
          }))
          .apply(TextIO.Write.named("Write Kmer Indices").to(options.kmerOutput));
    }
    
    // Figure this out later
    //kmers.apply(new OutputPcaFile(options.pcaOutput));
    
    p.run(PipelineRunner.fromOptions(options));
  }
}