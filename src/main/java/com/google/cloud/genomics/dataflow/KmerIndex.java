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

// project id 107046053965
// test dataset 13770895782338053201
// listeria dataset 13548522727457381097
// salmonella dataset 2831627299882627465

package com.google.cloud.genomics.dataflow;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.Readset;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.api.services.genomics.model.SearchReadsResponse;
import com.google.api.services.genomics.model.SearchReadsetsRequest;
import com.google.api.services.genomics.model.SearchReadsetsResponse;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.Description;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.utils.OptionsParser;
import com.google.cloud.dataflow.utils.RequiredOption;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.functions.OutputPcaFile;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.logging.Logger;

/**
 * Dataflows pipeline for generating kmer indices of all genomes filed under a given dataset id
 */
public class KmerIndex {
  private static final Logger LOG = Logger.getLogger(KmerIndex.class.getName());
  private static final String READSET_FIELDS = "nextPageToken,readsets(id,name)";
  private static final String READ_FIELDS = "nextPageToken,reads(originalBases)";
  private static int K_VALUE;

  // Do not instantiate
  private KmerIndex() { }

  private static class Options extends PipelineOptions {
    @Description("The dataset to read variants from")
    @RequiredOption
    public String datasetId = null;

    @Description("If querying a public dataset, provide a Google API key that has access "
        + "to variant data and no OAuth will be performed.")
    public String apiKey = null;

    @Description("If querying private datasets, or performing any write operations, "
        + "you need to provide the path to client_secrets.json. Do not supply an api key.")
    public String clientSecretsFilename = "client_secrets.json";

    @Description("Path of the file to write to")
    @RequiredOption
    public String output;
    
    @Description("Numerical value of K for indexing. Should not be larger than ~100")
    @RequiredOption
    public String kValue;
    
    public String accessToken = null;
    
    public void setAccessToken(String token) {
      accessToken = token;
    }
  }
  
  private static DoFn<Readset, KV<String, Read>> readsetToRead(
      final String accessToken, final String apiKey) {
    return new DoFn<Readset, KV<String, Read>>() {
      @Override
      public void processElement(ProcessContext c) {
        GenomicsApi api = new GenomicsApi(accessToken, apiKey);

        Readset set = c.element();
        SearchReadsRequest request = new SearchReadsRequest().setReadsetIds(Lists.newArrayList(set.getId()));
        boolean done = false;
        int count = 0;
        while (!done) {
          SearchReadsResponse response;
          try {
            response = api.executeRequest(api.getService().reads().search(request), READ_FIELDS);
          } catch (IOException e) {
            throw new RuntimeException("Failed to create genomics API request - this shouldn't happen.", e);
          }

          for (Read read : response.getReads()) {
            c.output(KV.of(set.getName(), read));
            count++;
          }
          
          LOG.info("Read " + response.getReads().size() + " reads, on page with"
              + " token " + request.getPageToken());
          LOG.info("Total reads: " + count);
          LOG.info("Next page token: " + response.getNextPageToken());
          
          if (response.getNextPageToken() != null) {
            request.setPageToken(response.getNextPageToken());
          } else {
            done = true;
          }
          
          if (count > 10000) {
            done = true;
          }
        }
      }
    };
  }

  private static DoFn<KV<String, Read>, KV<String, String>> genKmers = 
      new DoFn<KV<String, Read>, KV<String, String>>() {

    @Override
    public void processElement(ProcessContext c) {
      String name = c.element().getKey();
      String seq = c.element().getValue().getOriginalBases();
      StringBuilder kmer = new StringBuilder(K_VALUE + 1);
      int skip = 0;
      for (int i = 0; i < seq.length(); i++) {
        kmer.append(seq.charAt(i));
        // Skip all k-mers with unknown nucleotides
        if (seq.charAt(i) == 'N') {
          skip = K_VALUE;
        }
        
        if (kmer.length() == K_VALUE + 1) {
          kmer.deleteCharAt(0);
          if (skip == 0) {
            c.output(KV.of(name, kmer.toString()));
          }
        }
        
        if (skip > 0) {
          skip--;
        }
      }
    }
  };

  private static List<Readset> getReadsets(Options options) throws IOException {
    GenomicsApi api = new GenomicsApi(options.accessToken, options.apiKey);
    SearchReadsetsRequest request = new SearchReadsetsRequest()
        .setDatasetIds(Lists.newArrayList(options.datasetId));
    List<Readset> readsets = Lists.newArrayList();
    boolean done = false;
    while (!done) {
      SearchReadsetsResponse response = api.executeRequest(api.getService().readsets().search(request), READSET_FIELDS);
      
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
    try {
      K_VALUE = Integer.parseInt(options.kValue);
      if(K_VALUE <= 0) {
        LOG.severe("Invalid K value");
        throw new IllegalArgumentException("K value must be a positive integer");
      }
    } catch (NumberFormatException e) {
      LOG.severe("Invalid K value");
      throw new IllegalArgumentException("K value must be a positive integer");
    }
    
    if (options.apiKey == null) {
      try {
        LOG.info("Using client secrets file for OAuth");
        options.setAccessToken(GenomicsApi.getAccessToken(options.clientSecretsFilename,
            Lists.newArrayList("https://www.googleapis.com/auth/genomics")));
      } catch (GeneralSecurityException | IOException e) {
        LOG.severe("Error generating auth tokens");
        e.printStackTrace();
        return;
      }
    }
    
    LOG.info("Starting pipeline...");
    List<Readset> readsets = getReadsets(options);
    
    Pipeline p = Pipeline.create();

    DataflowWorkarounds.getPCollection(
        readsets, GenericJsonCoder.of(Readset.class), p, options.numWorkers)
        .apply(ParDo.named("Readsets To Reads")
            .of(readsetToRead(options.accessToken, options.apiKey)))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), GenericJsonCoder.of(Read.class)))
        .apply(ParDo.named("Generate Kmers").of(genKmers))
        .apply(new OutputPcaFile(options.output));
    
    p.run(PipelineRunner.fromOptions(options));
  }
}