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

package com.google.cloud.genomics.dataflow;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.extensions.java6.auth.oauth2.GooglePromptReceiver;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.runners.Description;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.utils.OptionsParser;
import com.google.common.collect.Lists;

import java.io.*;
import java.security.GeneralSecurityException;
import java.util.List;

/**
 * An pipeline that generates similarity data for variants in a dataset.
 *
 * To access the Google Genomics API, you need to either provide a valid API key
 * or a client_secrets.json file.
 *
 * Follow the instructions at http://developers.google.com/genomics
 * to generate a client_secrets.json file, and move it into the directory that you run the pipeline from.
 *
 * Alternatively, specify the API key with this flag:
 *   --apiKey=<API_KEY>
 *
 *
 * To execute this pipeline locally, specify general pipeline configuration:
 *   --project=<PROJECT ID>  
 * and example configuration:
 *   --output=[<LOCAL FILE> | gs://<OUTPUT PATH>]
 *
 * To execute this pipeline using the Dataflow service, specify pipeline configuration:
 *   --project=<PROJECT ID> --stagingLocation=gs://<STAGING DIRECTORY> 
 *   --runner=BlockingDataflowPipelineRunner
 * and example configuration:
 *   --output=gs://<OUTPUT PATH>
 *
 * The input datasetId defaults to 376902546192 (1000 genomes) and can be
 * overridden with --datasetId.
 */
public class VariantSimilarity {
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  /** Emits a callset pair every time they share a variant. */
  public static class ExtractSimilarCallsets extends DoFn<Variant, KV<String, String>> {

    @Override
    public void processElement(ProcessContext c) {
      Variant variant = c.element();
      List<String> samplesWithVariant = Lists.newArrayList();
      for (Call call : variant.getCalls()) {
        String genotype = call.getInfo().get("GT").get(0); // TODO: Change to use real genotype field
        genotype = genotype.replaceAll("[\\\\|0]", "");
        if (!genotype.isEmpty()) {
          samplesWithVariant.add(call.getCallsetName());
        }
      }

      for (String s1 : samplesWithVariant) {
        for (String s2 : samplesWithVariant) {
          c.output(KV.of(s1, s2));
        }
      }
    }
  }

  private static List<VariantReader.Options> getReaderOptions(Options options)
      throws GeneralSecurityException, IOException {
    String variantFields = "nextPageToken,variants(id,calls(info,callsetName))";

    // Get an access token only if an apiKey is not supplied
    String accessToken = null;
    if (options.apiKey == null) {
      accessToken = getAccessToken(options, Lists.newArrayList("https://www.googleapis.com/auth/genomics"));
    }


    // TODO: Get contig bounds here vs hardcoding. Eventually this will run over all available data within a dataset
    // NOTE: Locally, we can only do about 1k bps before we run out of memory
    String contig = "22";
    long start = 25652000;
    long end = start + 500;
    long basesPerShard = 1000;

    double shards = Math.ceil((end - start) / (double) basesPerShard);
    List<VariantReader.Options> readers = Lists.newArrayList();
    for (int i = 0; i < shards; i++) {
      long shardStart = start + (i * basesPerShard);
      long shardEnd = Math.min(end, shardStart + basesPerShard);

      System.out.println("Adding reader with " + shardStart + " to " + shardEnd);
      readers.add(new VariantReader.Options(options.apiKey, accessToken, options.datasetId, variantFields,
          contig, shardStart, shardEnd));
    }
    return readers;
  }

  private static GoogleClientSecrets loadClientSecrets(String clientSecretsFilename) throws IOException {
    File f = new File(clientSecretsFilename);
    if (f.exists()) {
      InputStream inputStream = new FileInputStream(new File(clientSecretsFilename));
      return GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(inputStream));
    }

    throw new RuntimeException("Please provide an --apiKey option or a valid client_secrets.json file."
        + " Client secrets file " + clientSecretsFilename + " does not exist."
        + " Visit https://developers.google.com/genomics to learn how to get an api key or"
        + " install a client_secrets.json file. If you have installed a client_secrets.json"
        + " in a specific location, use --clientSecretsFilename <path>/client_secrets.json.");
  }

  private static String getAccessToken(Options options, List<String> scopes)
      throws GeneralSecurityException, IOException {
    GoogleClientSecrets clientSecrets = loadClientSecrets(options.clientSecretsFilename);

    NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
        httpTransport, JSON_FACTORY, clientSecrets, scopes).setAccessType("offline").build();
    Credential credential = new AuthorizationCodeInstalledApp(flow, new GooglePromptReceiver()).authorize("user");
    return credential.getAccessToken();
  }

  private static class Options extends PipelineOptions {
    @Description("The dataset to read variants from")
    public String datasetId = "376902546192"; // 1000 genomes

    @Description("If querying a public dataset, provide a Google API key that has access " +
        "to variant data and no OAuth will be performed.")
    public String apiKey = null;

    @Description("If querying private datasets, or performing any write operations, " +
        "you need to provide the path to client_secrets.json. Do not supply an api key.")
    public String clientSecretsFilename = "client_secrets.json";

    @Description("Path of the file to write to")
    public String output;

    public String getOutput() {
      if (output != null) {
        return output;
      } else {
        throw new IllegalArgumentException("Must specify --output");
      }
    }
  }

  public static void main(String[] args) throws GeneralSecurityException, IOException {
    Options options = OptionsParser.parse(args, Options.class, VariantSimilarity.class.getSimpleName());
    List<VariantReader.Options> readerOptions = getReaderOptions(options);

    Pipeline p = Pipeline.create();
    p.begin()
        .apply(Create.of(readerOptions)).setCoder(SerializableCoder.of(VariantReader.Options.class))
        .apply(ParDo.named("VariantFetcher")
            .of(new VariantReader.GetVariants())).setCoder(GenericJsonCoder.of(Variant.class))
        .apply(ParDo.named("ExtractSimilarCallsets").of(new ExtractSimilarCallsets()))
        .apply(new Count<KV<String, String>>())
        .apply(ParDo.named("FormatCallsetCounts").of(new DoFn<KV<KV<String, String>, Long>, String>() {
          @Override
          public void processElement(ProcessContext c) {
            KV<String, String> callsets = c.element().getKey();
            Long count = c.element().getValue();
            // TODO: Hook the pca code itself into the pipeline rather than writing a file with this intermediate data
            c.output(callsets.getKey() + "-" + callsets.getValue() + "-" + count + ":");
          }
        }))
        .apply(TextIO.Write.named("WriteCounts").to(options.getOutput()));

    p.run(PipelineRunner.fromOptions(options));
  }
}
 
