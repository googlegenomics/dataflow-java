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

import com.google.api.services.genomics.model.ContigBound;
import com.google.api.services.genomics.model.GetVariantsSummaryResponse;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.Description;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.utils.OptionsParser;
import com.google.cloud.dataflow.utils.RequiredOption;
import com.google.cloud.genomics.dataflow.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.GenomicsApi;
import com.google.cloud.genomics.dataflow.GenomicsOptions;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.functions.ExtractSimilarCallsets;
import com.google.cloud.genomics.dataflow.functions.OutputPCoAFile;
import com.google.cloud.genomics.dataflow.readers.VariantReader;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * An pipeline that generates similarity data for variants in a dataset.
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
public class VariantSimilarity {
  private static final Logger LOG = Logger.getLogger(VariantSimilarity.class.getName());

  private static List<SearchVariantsRequest> getVariantRequests(GetVariantsSummaryResponse summary,
      Options options) throws IOException {

    ContigBound bound = summary.getContigBounds().get(0);

    // TODO: Run this over all of the available contigBounds
    // We just happen to know that there is data at this hardcoded start position
    // NOTE: The default end parameter is set to run on tiny local machines
    String contig = bound.getContig();
    long start = 25652000;
    long end = start + 500;
    long basesPerShard = 1000;

    double shards = Math.ceil((end - start) / (double) basesPerShard);
    List<SearchVariantsRequest> requests = Lists.newArrayList();
    for (int i = 0; i < shards; i++) {
      long shardStart = start + (i * basesPerShard);
      long shardEnd = Math.min(end, shardStart + basesPerShard);

      LOG.info("Adding request with " + shardStart + " to " + shardEnd);
      requests.add(new SearchVariantsRequest()
          .setDatasetId(options.datasetId)
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
  }

  public static void main(String[] args) throws IOException {
    Options options = OptionsParser.parse(args, Options.class,
        VariantSimilarity.class.getSimpleName());

    String accessToken = options.getAccessToken();
    String variantFields = "nextPageToken,variants(id,calls(info,callsetName))";

    GenomicsApi api = new GenomicsApi(accessToken, options.apiKey);
    GetVariantsSummaryResponse summary = api.executeRequest(
        api.getService().variants().getSummary().setDatasetId(options.datasetId), "contigBounds");

    List<SearchVariantsRequest> requests = getVariantRequests(summary, options);

    Pipeline p = Pipeline.create();

    DataflowWorkarounds.getPCollection(requests,
        GenericJsonCoder.of(SearchVariantsRequest.class), p, options.numWorkers)
        .apply(ParDo.named("VariantReader")
            .of(new VariantReader(accessToken, options.apiKey, variantFields)))
            .setCoder(GenericJsonCoder.of(Variant.class))
        .apply(ParDo.named("ExtractSimilarCallsets").of(new ExtractSimilarCallsets()))
        .apply(new OutputPCoAFile(options.output));

    p.run(PipelineRunner.fromOptions(options));
  }
}
 
