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

import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.ContigBound;
import com.google.api.services.genomics.model.GetVariantsSummaryResponse;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.Description;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.utils.OptionsParser;
import com.google.cloud.dataflow.utils.RequiredOption;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.functions.ExtractFamilyVariantStatus;
import com.google.cloud.genomics.dataflow.readers.VariantReader;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsAuth;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * A pipeline that calculates the transmission probability of each allele in parents.
 *
 * To access the Google Genomics API, you need to either provide a valid API key
 * or a client_secrets.json file.
 *
 * Follow the instructions at http://developers.google.com/genomics
 * to generate a client_secrets.json file, and move it into the directory that you run
 * the pipeline from.
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
public class TransmissionProbability {
  private static final Logger LOG = Logger.getLogger(TransmissionProbability.class.getName());
  private static final String VARIANT_FIELDS = "nextPageToken,variants(id,names,calls(info,callsetName))";

  private static List<SearchVariantsRequest> getVariantRequests(
      GetVariantsSummaryResponse summary, Options options) {

    // TODO: Run this over all of the available contigBounds
    ContigBound bound = summary.getContigBounds().get(0);
    String contig = bound.getContig();
    // NOTE: The default end parameter is set to run on tiny local machines
    long start = 25652000;
    long end = start + 10000;
    long basesPerShard = 1000;

    double shards = Math.ceil((end - start) / (double) basesPerShard);
    List<SearchVariantsRequest> requests = Lists.newArrayList();
    for (int i = 0; i < shards; i++) {
      long shardStart = start + (i * basesPerShard);
      long shardEnd = Math.min(end, shardStart + basesPerShard);

      LOG.info("Adding request with " + shardStart + " to " + shardEnd);
      requests.add(new SearchVariantsRequest()
          .setVariantsetId(options.datasetId)
          .setContig(contig)
          .setStartPosition(shardStart)
          .setEndPosition(shardEnd));

    }
    return requests;
  }

  private static class Options extends GenomicsOptions {
    @Description("Path of the file to write to")
    @RequiredOption
    public String output;
    
    @Description("The ID of the Google Genomics dataset this pipeline is working with. " +
        "Defaults to Platinum genomes dataset.")
    public String datasetId = "14004469326575082626";
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException, GeneralSecurityException {
    Options options = OptionsParser.parse(args, Options.class,
        TransmissionProbability.class.getSimpleName());
    options.validateOptions();

    GenomicsAuth auth = options.getGenomicsAuth();

    GetVariantsSummaryResponse summary = auth.getService().variants().getSummary()
        .setVariantsetId(options.datasetId).setFields("contigBounds").execute();

    List<SearchVariantsRequest> requests = getVariantRequests(summary, options);

    Pipeline p = Pipeline.create();
    DataflowWorkarounds.registerGenomicsCoders(p);

    // The below pipeline works as follows:
    //    - Fetch the variants,
    //    - For each variant, see which parent transferred the variant to the
    //        child.
    //    - Groups Transmission sources by Variant,
    //    - Calculate transmission Probability for each variant
    //    - Print calculated values to a file.
    DataflowWorkarounds.getPCollection(requests, p, options.numWorkers)
        .apply(ParDo.named("VariantReader")
            .of(new VariantReader(auth, VARIANT_FIELDS)))
        .apply(ParDo.named("ExtractFamilyVariantStatus")
            .of(new ExtractFamilyVariantStatus()))
        .apply(GroupByKey.<String, Boolean>create())
        .apply(ParDo.named("CalculateTransmissionProbability")
            .of(new DoFn<KV<String, Iterable<Boolean>>,
                         KV<String, Double>>() {
          @Override
          public void processElement(ProcessContext c) {
            KV<String, Iterable<Boolean>> input = c.element();
            long m = 0,f = 0;
            for (Boolean b : input.getValue()) {
              if (b) {
                m++;
              } else {
                f++;
              }
            }
            double tdt = Math.pow(m - f, 2.0) / (m + f);
            c.output(KV.of(input.getKey(), tdt));
          }
        }))
        .apply(ParDo.named("WriteDataToFile")
            .of(new DoFn<KV<String, Double>, String>() {
          @Override
          public void processElement(ProcessContext c) {
            KV<String, Double> pair = c.element();
            String output = pair.getKey() + "\t" + pair.getValue() + "\n";
            c.output(output);
          }
        }))
        .apply(TextIO.Write.named("WriteCounts").to(options.output));

    p.run(DataflowWorkarounds.getRunner(options));
  }
}
 
