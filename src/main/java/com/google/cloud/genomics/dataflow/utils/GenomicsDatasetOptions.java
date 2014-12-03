/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dataflow.utils;

import com.google.api.services.genomics.model.ReferenceBound;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.api.services.genomics.model.VariantSet;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.genomics.dataflow.model.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import static com.google.common.collect.Lists.newArrayList;

/**
 * A common options class for all pipelines that operate over a single dataset and write their
 * analysis to a file.
 */
public interface GenomicsDatasetOptions extends GenomicsOptions {

  // If not running all contigs, we default to BRCA1
  // TODO: Look up the valid value for '17' from the referenceBounds call
  public static final String DEFAULT_REFERNCES = "17:41196311:41277499";

  public static final long DEFAULT_NUMBER_OF_BASES_PER_SHARD = 100000;

  public static class Methods {

    private static final Logger LOG = Logger.getLogger(GenomicsDatasetOptions.class.getName());

    // TODO: If needed, add getReadRequests method
    private static List<SearchVariantsRequest> getShardedRequests(String variantSetId,
        Contig contig, long numberOfBasesPerShard) {

      double shards =
          Math.ceil((contig.getEnd() - contig.getStart()) / (double) numberOfBasesPerShard);
      List<SearchVariantsRequest> requests = Lists.newArrayList();
      for (int i = 0; i < shards; i++) {
        long shardStart = contig.getStart() + (i * numberOfBasesPerShard);
        long shardEnd = Math.min(contig.getEnd(), shardStart + numberOfBasesPerShard);

        LOG.info("Adding request with " + contig.getReferenceName() + " " + shardStart + " to "
            + shardEnd);
        requests.add(new SearchVariantsRequest()
            .setVariantSetIds(Collections.singletonList(variantSetId))
            .setReferenceName(contig.getReferenceName())
            .setStart(shardStart)
            .setEnd(shardEnd));
      }
      return requests;
    }

    private static List<SearchVariantsRequest> getShardedRequests(String variantSetId,
        Iterable<Contig> contigs, long numberOfBasesPerShard) {
      List<SearchVariantsRequest> requests = newArrayList();
      for (Contig contig : contigs) {
        requests.addAll(getShardedRequests(variantSetId, contig, numberOfBasesPerShard));
      }
      return requests;
    }

    public static List<SearchVariantsRequest> getVariantRequests(GenomicsDatasetOptions options,
        GenomicsFactory.OfflineAuth auth) throws IOException, GeneralSecurityException {
      String datasetId = options.getDatasetId();
      List<SearchVariantsRequest> requests = Lists.newArrayList();

      if (options.isAllContigs()) {
        VariantSet variantSet =
            auth.getGenomics(auth.getDefaultFactory()).variantsets().get(datasetId).execute();
        for (ReferenceBound bound : variantSet.getReferenceBounds()) {
          String contig = bound.getReferenceName().toLowerCase();
          if (contig.contains("x") || contig.contains("y")) {
            // X and Y skew analysis results
            continue;
          }

          requests.addAll(getShardedRequests(variantSet.getId(),
              new Contig(bound.getReferenceName(), 0, bound.getUpperBound()),
              options.getNumberOfBasesPerShard()));
        }

      } else {
        requests = getShardedRequests(datasetId, getContigs(options.getReferences()),
            options.getNumberOfBasesPerShard());
      }

      Collections.shuffle(requests); // Shuffle requests for better backend performance
      return requests;
    }

    static Iterable<Contig> getContigs(String contigs) {
      return Iterables.transform(Splitter.on(",").split(contigs), new Function<String, Contig>() {

        @Override
        public Contig apply(String contigString) {
          ArrayList<String> contigInfo = newArrayList(Splitter.on(":").split(contigString));
          return new Contig(contigInfo.get(0), Long.valueOf(contigInfo.get(1)), Long
              .valueOf(contigInfo.get(2)));
        }

      });
    }
  }

  @Description("The ID of the Google Genomics dataset this pipeline is working with. "
      + "Defaults to 1000 Genomes.")
  @Default.String("10473108253681171589")
  String getDatasetId();

  @Description("Path of the file to write to")
  String getOutput();

  @Description("By default, PCA will be run on BRCA1, pass this flag to run on all "
      + "non X and Y contigs present in the dataset")
  boolean isAllContigs();

  void setAllContigs(boolean allContigs);

  void setDatasetId(String datasetId);

  void setOutput(String output);

  @Description("Comma separated tuples of reference:start:end,... Defaults to " + DEFAULT_REFERNCES)
  @Default.String(DEFAULT_REFERNCES)
  String getReferences();

  void setReferences(String references);

  @Description("Number of bases per shard Defaults to " + DEFAULT_NUMBER_OF_BASES_PER_SHARD)
  @Default.Long(DEFAULT_NUMBER_OF_BASES_PER_SHARD)
  long getNumberOfBasesPerShard();

  void setNumberOfBasesPerShard(long numberOfBasesPerShard);
}
