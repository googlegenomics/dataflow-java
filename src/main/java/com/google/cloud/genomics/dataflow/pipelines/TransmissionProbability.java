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

import com.google.api.services.genomics.model.ReferenceBound;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.api.services.genomics.model.VariantSet;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.runners.Description;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.utils.OptionsParser;
import com.google.cloud.dataflow.utils.RequiredOption;
import com.google.cloud.genomics.dataflow.data_structures.Allele;
import com.google.cloud.genomics.dataflow.functions.CalculateTransmissionProbability;
import com.google.cloud.genomics.dataflow.functions.ExtractAlleleTransmissionStatus;
import com.google.cloud.genomics.dataflow.readers.VariantReader;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsAuth;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
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
      return getShardedRequests(options.datasetId, "17", 41196312, 41277500);
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
    public String datasetId = "14004469326575082626";

    @Description("By default, PCA will be run on BRCA1, pass this flag to run on all " +
        "non X and Y contigs present in the dataset")
    public boolean allContigs = false;
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException, GeneralSecurityException {
    Options options = OptionsParser.parse(args, Options.class,
        TransmissionProbability.class.getSimpleName());
    options.validateOptions();

    GenomicsAuth auth = options.getGenomicsAuth();

    List<SearchVariantsRequest> requests = getVariantRequests(auth, options);

    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);

    // The below pipeline works as follows:
    //    - Fetch the variants,
    //    - For each Variant, see which alleles were actually transmitted to
    //        the child
    //    - Groups Transmission sources by Variant,
    //    - Calculate transmission Probability for each variant
    //    - Print calculated values to a file.
    DataflowWorkarounds.getPCollection(requests, p, options.numWorkers)
        .apply(ParDo.named("VariantReader")
            .of(new VariantReader(auth, VARIANT_FIELDS)))
        .apply(ParDo.named("ExtractFamilyVariantStatus")
            .of(new ExtractAlleleTransmissionStatus()))
        .apply(GroupByKey.<Allele, Boolean>create())
        .apply(ParDo.named("CalculateTransmissionProbability")
            .of(new CalculateTransmissionProbability()))
        .apply(ParDo.named("WriteDataToFile")
            .of(new DoFn<KV<Allele, Double>, String>() {
          @Override
          public void processElement(ProcessContext c) {
            KV<Allele, Double> pair = c.element();
            String output = pair.getKey().getReferenceName() + "\t" +
                pair.getKey().getAllele() + "\t" + pair.getValue() + "\n";
            c.output(output);
          }
        }))
        .apply(TextIO.Write.named("WriteResults").to(options.output));

    p.run();
  }
}
