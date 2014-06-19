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

import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.Readset;
import com.google.api.services.genomics.model.SearchReadsetsRequest;
import com.google.api.services.genomics.model.SearchReadsetsResponse;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.Description;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.utils.OptionsParser;
import com.google.cloud.dataflow.utils.RequiredOption;
import com.google.cloud.genomics.dataflow.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.GenomicsApi;
import com.google.cloud.genomics.dataflow.GenomicsOptions;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.functions.OutputPcaFile;
import com.google.cloud.genomics.dataflow.functions.ReadsToKmers;
import com.google.cloud.genomics.dataflow.functions.ReadsetToRead;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * Dataflows pipeline for generating kmer indices of all genomes filed under a given dataset id
 */
public class KmerIndex {
  private static final Logger LOG = Logger.getLogger(KmerIndex.class.getName());
  private static final String READSET_FIELDS = "nextPageToken,readsets(id,name)";

  // Do not instantiate
  private KmerIndex() { }

  private static class Options extends GenomicsOptions {
    @Description("Path of the file to write to")
    @RequiredOption
    public String output;
    
    @Description("Numerical value of K for indexing. Should not be larger than ~100")
    @RequiredOption
    public Integer kValue;
    
    public void checkArgs() {
      if(kValue == null || kValue < 1 || kValue > 256) {
        throw new IllegalArgumentException("K value is out of bounds (must be between 1 and 256");
      }
    }
  }

  private static List<Readset> getReadsets(Options options) throws IOException {
    GenomicsApi api = new GenomicsApi(options.getAccessToken(), options.apiKey);
    SearchReadsetsRequest request = new SearchReadsetsRequest()
        .setDatasetIds(Lists.newArrayList(options.datasetId));
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
    Options options = OptionsParser.parse(args, Options.class, KmerIndex.class.getSimpleName());
    options.checkArgs();
    
    LOG.info("Starting pipeline...");
    List<Readset> readsets = getReadsets(options);
    
    Pipeline p = Pipeline.create();

    DataflowWorkarounds.getPCollection(
        readsets, GenericJsonCoder.of(Readset.class), p, options.numWorkers)
        .apply(ParDo.named("Readsets To Reads")
            .of(new ReadsetToRead(options)))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), GenericJsonCoder.of(Read.class)))
        .apply(ParDo.named("Generate Kmers").of(new ReadsToKmers(options.kValue)))
        .apply(new OutputPcaFile(options.output));
    
    p.run(PipelineRunner.fromOptions(options));
  }
}