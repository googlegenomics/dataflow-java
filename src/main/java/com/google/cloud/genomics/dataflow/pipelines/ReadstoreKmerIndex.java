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
import com.google.cloud.dataflow.sdk.runners.Description;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.utils.OptionsParser;
import com.google.cloud.dataflow.utils.RequiredOption;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.functions.GenerateKmers;
import com.google.cloud.genomics.dataflow.functions.WriteKmers;
import com.google.cloud.genomics.dataflow.readers.ReadsetToReads;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsApi;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.logging.Logger;

/**
 * Dataflows pipeline for performing PCA on generated kmer indicies from the reads in a dataset
 */
public class ReadstoreKmerIndex {
  private static final Logger LOG = Logger.getLogger(ReadstoreKmerIndex.class.getName());
  private static final String READSET_FIELDS = "nextPageToken,readsets(id,name)";
  private static final String READ_FIELDS = "nextPageToken,reads(originalBases)";

  // Do not instantiate
  private ReadstoreKmerIndex() { }

  private static class Options extends GenomicsOptions {
    @Description("Path of GCS directory to write results to")
    @RequiredOption
    public String outputLocation;
    
    @Description("Whether or not kmer indices should be printed as table instead or entries\n"
        + "Note: table does not work for k > 8 due to Dataflow limitations")
    public boolean writeTable;
    
    @Description("Prefix to be used for output file. Files written will be in the form"
        + "<outputPrefix>K<KValue>.csv/txt. Default value is KmerIndex")
    public String outputPrefix = "KmerIndex";
    
    @Description("K values to be used for indexing. Separate multiple values using commas\n"
        + "EG: --kValues=1,2,3")
    @RequiredOption
    public String kValues;
    
    private int[] parsedValues;
    
    public void checkArgs() {
      try {
        for (int val : parseValues()) {
          if (val < 1 || val > 256) {
            LOG.severe("K values must be between 1 and 256");
            throw new IllegalArgumentException("K value out of bounds");
          }
        }
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid K values");
      }
    }
    
    public int[] parseValues() throws NumberFormatException {
      if (parsedValues != null) {
        return parsedValues;
      }
      
      String[] values = kValues.split(",");
      parsedValues = new int[values.length];
      for (int i = 0; i < values.length; i++) {
        parsedValues[i] = Integer.parseInt(values[i]);
      }
      
      return parsedValues;
    }
  }

  private static List<Readset> getReadsets(
      String accessToken, String apiKey, String datasetId) throws IOException {
    GenomicsApi api = new GenomicsApi(accessToken, apiKey);
    SearchReadsetsRequest request = new SearchReadsetsRequest()
        .setDatasetIds(Lists.newArrayList(datasetId))
        .setMaxResults(new BigInteger("256"));
    List<Readset> readsets = Lists.newArrayList();
    do {
      SearchReadsetsResponse response = api.executeRequest(
          api.getService().readsets().search(request), READSET_FIELDS);
      readsets.addAll(response.getReadsets());
      request.setPageToken(response.getNextPageToken());
    } while (request.getPageToken() != null);
    return readsets;
  }
  
  public static void main(String[] args) throws IOException {
    Options options = OptionsParser.parse(
        args, Options.class, ReadstoreKmerIndex.class.getSimpleName());
    options.checkArgs();
    
    int[] kValues = options.parseValues();

    LOG.info("Starting pipeline...");
    String token = options.getAccessToken();
    List<Readset> readsets = getReadsets(token, options.apiKey, options.datasetId);
    
    Pipeline p = Pipeline.create();
    
    PCollection<KV<String, String>> reads = DataflowWorkarounds.getPCollection(
        readsets, GenericJsonCoder.of(Readset.class), p, options.numWorkers)
        .apply(ParDo.named("Readsets To Reads")
            .of(new ReadsetToReads(token, options.apiKey, READ_FIELDS)));
    
    for (int kValue : kValues) {
      String outFile = options.outputLocation + "/" + options.outputPrefix + "K" + kValue;
      outFile += (options.writeTable) ? ".csv" : ".txt";
      reads.apply(ParDo.named("Generate Kmers")
          .of(new GenerateKmers(kValue)))
          .apply(new WriteKmers(outFile, options.writeTable));
    }
    
    p.run(PipelineRunner.fromOptions(options));
  }
}