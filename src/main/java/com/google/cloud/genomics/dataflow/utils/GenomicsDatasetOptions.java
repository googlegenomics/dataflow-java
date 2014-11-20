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
package com.google.cloud.genomics.dataflow.utils;

import com.google.api.services.genomics.model.ReferenceBound;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.api.services.genomics.model.VariantSet;
import com.google.cloud.dataflow.sdk.runners.Description;
import com.google.cloud.dataflow.utils.RequiredOption;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
* A common options class for all pipelines that operate over a single dataset and write
 * their analysis to a file.
*/
public class GenomicsDatasetOptions extends GenomicsOptions {
  private static final Logger LOG = Logger.getLogger(GenomicsDatasetOptions.class.getName());

  @Description("Path of the file to write to")
  @RequiredOption
  public String output;

  @Description("The ID of the Google Genomics dataset this pipeline is working with. " +
      "Defaults to 1000 Genomes.")
  public String datasetId = "10473108253681171589";

  @Description("By default, PCA will be run on BRCA1, pass this flag to run on all " +
      "non X and Y contigs present in the dataset")
  public boolean allContigs = false;


  public List<SearchVariantsRequest> getVariantRequests(GenomicsAuth auth)
      throws IOException, GeneralSecurityException {
    if (allContigs) {
      List<SearchVariantsRequest> requests = Lists.newArrayList();

      VariantSet variantSet = auth.getService().variantsets().get(datasetId).execute();
      for (ReferenceBound bound : variantSet.getReferenceBounds()) {
        String contig = bound.getReferenceName().toLowerCase();
        if (contig.contains("x") || contig.contains("y")) {
          // X and Y skew analysis results
          continue;
        }

        requests.addAll(getShardedRequests(variantSet.getId(),
            bound.getReferenceName(), 0, bound.getUpperBound()));
      }
      return requests;

    } else {
      // If not running all contigs, we default to BRCA1
      // TODO: Look up the valid value for '17' from the referenceBounds call
      return getShardedRequests(datasetId, "17", 41196312, 41277500);
    }
  }

  // TODO: If needed, add getReadRequests method

  private List<SearchVariantsRequest> getShardedRequests(String variantSetId, String contig,
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

}
