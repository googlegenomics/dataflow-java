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
// real dataset 13548522727457381097

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
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.runners.Description;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.utils.OptionsParser;
import com.google.cloud.dataflow.utils.RequiredOption;
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
  private static final int API_RETRIES = 3;
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
        Genomics service = setAuth(accessToken);
        Readset set = c.element();
        SearchReadsRequest request = new SearchReadsRequest()
        .setReadsetIds(Lists.newArrayList(set.getId()));
        boolean done = false;
        int count = 0;
        while (!done) {
          SearchReadsResponse response = null;
          for (int i = 0; i < API_RETRIES; i++) {
            try {
              response = service.reads().search(request)
                  .setFields(READ_FIELDS).setKey(apiKey).execute();
              break;
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
          if (response == null) {
            throw new RuntimeException("Genomics API call failed multiple times in a row.");
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
      StringBuffer buf = new StringBuffer(K_VALUE + 1);
      for (int i = 0; i < seq.length(); i++) {
        buf.append(seq.charAt(i));
        if (buf.length() == K_VALUE + 1) {
          buf.deleteCharAt(0);
          c.output(KV.of(name, buf.toString()));
        }
      }
    }
  };
  
  private static DoFn<KV<KV<String, String>, Long>, String> formatOutput =
      new DoFn<KV<KV<String, String>, Long>, String> () {

        @Override
        public void processElement(ProcessContext c) {
          KV<KV<String, String>, Long> elem = c.element();
          String name = elem.getKey().getKey();
          String kmer = elem.getKey().getValue();
          Long count = elem.getValue();
          c.output(name + "-" + kmer + "-" + count + ":");
        }
  };

  private static Genomics setAuth(String accessToken) {
    GoogleCredential credential = (accessToken == null) ? null :
        new GoogleCredential().setAccessToken(accessToken);
    try {
      return new Genomics.Builder(GoogleNetHttpTransport.newTrustedTransport(), 
          new JacksonFactory(), credential)
      .setApplicationName("dataflow-reader").build();
    } catch (GeneralSecurityException | IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private static List<Readset> getReadsets(Options options) {
    Genomics service = setAuth(options.accessToken);
    SearchReadsetsRequest request = new SearchReadsetsRequest()
        .setDatasetIds(Lists.newArrayList(options.datasetId));
    List<Readset> readsets = Lists.newArrayList();
    boolean done = false;
    while (!done) {
      SearchReadsetsResponse response = null;
      
      for (int i = 0; i < API_RETRIES; i++) {
        try {
          response = service.readsets().search(request)
              .setFields(READSET_FIELDS).setKey(options.apiKey).execute();
          break;
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      if (response == null) {
        throw new RuntimeException("Genomics API call failed multiple times in a row.");
      }
      
      readsets.addAll(response.getReadsets());
      if (response.getNextPageToken() != null) {
        request.setPageToken(response.getNextPageToken());
      } else {
        done = true;
      }
    }
    return readsets;
  }
  
  public static void main(String[] args) {
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
        options.setAccessToken(new OAuthHelper().getAccessToken(options.clientSecretsFilename,
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
        .apply(Count.<KV<String, String>>create())
        .apply(ParDo.named("Format Output").of(formatOutput))
        .apply(TextIO.Write.named("WriteCounts").to(options.output));
    
    p.run(PipelineRunner.fromOptions(options));
  }
}