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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import com.google.api.services.genomics.model.ReferenceBound;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.api.services.genomics.model.VariantSet;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.runners.Description;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.utils.OptionsParser;
import com.google.cloud.dataflow.utils.RequiredOption;
import com.google.cloud.genomics.dataflow.functions.FormatIBSData;
import com.google.cloud.genomics.dataflow.functions.SharedAllelesCallsOfVariantCounter;
import com.google.cloud.genomics.dataflow.readers.VariantReader;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsAuth;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.common.collect.Lists;

/**
 * An pipeline that computes Identity by State (IBS) for variants in a dataset.
 *
 * To access the Google Genomics API, you need to either provide a valid API key or a
 * client_secrets.json file.
 *
 * Follow the instructions at http://developers.google.com/genomics to generate a
 * client_secrets.json file, and move it into the directory that you run the pipeline from.
 *
 * Alternatively, specify the API key with this flag: --apiKey=<API_KEY>
 *
 *
 * To execute this pipeline locally, specify general pipeline configuration: --project=<PROJECT ID>
 * and example configuration: --output=[<LOCAL FILE> | gs://<OUTPUT PATH>]
 *
 * To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * --project=<PROJECT ID> --stagingLocation=gs://<STAGING DIRECTORY>
 * --runner=BlockingDataflowPipelineRunner and example configuration: --output=gs://<OUTPUT PATH>
 *
 * The input datasetId defaults to 10473108253681171589 (1000 genomes) and can be overridden with
 * --datasetId.
 */
public class IdentityByState {
  private static final Logger LOG = Logger.getLogger(IdentityByState.class.getName());
  private static final String VARIANT_FIELDS = "nextPageToken,variants(id,calls(genotype,callSetName))";

  private static List<SearchVariantsRequest> getVariantRequests(GenomicsAuth auth, Options options)
      throws IOException, GeneralSecurityException {
    if (options.allContigs) {
      List<SearchVariantsRequest> requests = Lists.newArrayList();

      VariantSet variantSet = auth.getService().variantsets().get(options.datasetId).execute();
      for (ReferenceBound bound : variantSet.getReferenceBounds()) {
        String contig = bound.getReferenceName().toLowerCase();
        if (contig.contains("x") || contig.contains("y")) {
          // X and Y skew PCA results
          continue;
        }

        requests.addAll(getShardedRequests(variantSet.getId(),
            bound.getReferenceName(), 0, bound.getUpperBound()));
      }
      return requests;

    } else {
      // If not running all contigs, we default to BRCA1
      return getShardedRequests(options.datasetId, "chr17", 41196312, 41277500);
    }
  }

  private static List<SearchVariantsRequest> getShardedRequests(String variantSetId, String contig,
      long start, long end) {

    long basesPerShard = 1000000; // 1 million

    double shards = Math.ceil((end - start) / (double) basesPerShard);
    List<SearchVariantsRequest> requests = Lists.newArrayList();
    for (int i = 0; i < shards; i++) {
      long shardStart = start + (i * basesPerShard);
      long shardEnd = Math.min(end, shardStart + basesPerShard);

      LOG.info("Adding request with " + contig + " " + shardStart + " to " + shardEnd);
      requests.add(new SearchVariantsRequest()
          .setVariantSetIds(Collections.singletonList(variantSetId))
          .setReferenceName(contig)
          .setStart(shardStart)
          .setEnd(shardEnd));

    }
    return requests;
  }

  private static class Options extends GenomicsOptions {
    @Description("Path of the file to write to")
    @RequiredOption
    public String output;

    @Description("The ID of the Google Genomics dataset this pipeline is working with. " +
        "Defaults to 1000 Genomes.")
    public String datasetId = "10473108253681171589";

    @Description("By default, PCA will be run on BRCA1, pass this flag to run on all " +
        "non X and Y contigs present in the dataset")
    public boolean allContigs = false;
  }

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    Options options = OptionsParser.parse(args, Options.class,
        IdentityByState.class.getSimpleName());
    options.validateOptions();

    GenomicsAuth auth = options.getGenomicsAuth();

    List<SearchVariantsRequest> requests = getVariantRequests(auth, options);

    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);
    DataflowWorkarounds
        .getPCollection(requests, p, options.numWorkers)
        .apply(ParDo.named("VariantReader").of(new VariantReader(auth, VARIANT_FIELDS)))
        .apply(
            ParDo.named("SharedAllelesCallsOfVariantCounter").of(
                new SharedAllelesCallsOfVariantCounter()))
        .apply(Combine.<KV<String, String>, Double>perKey(new IBSCalculator()))
        .apply(ParDo.named("FormatIBSData").of(new FormatIBSData()))
        .apply(TextIO.Write.named("WriteIBS").to(options.output));

    p.run();
  }
}
