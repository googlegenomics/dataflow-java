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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.logging.Logger;

import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.Contig.SexChromosomeFilter;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A common options class for all pipelines that operate over a single dataset and write their
 * analysis to a file.
 */
public interface GenomicsDatasetOptions extends GenomicsOptions {
  public static class Methods {

    private static final Logger LOG = Logger.getLogger(GenomicsDatasetOptions.class.getName());

    // TODO: If needed, add getReadRequests method
    public static List<SearchVariantsRequest> getVariantRequests(GenomicsDatasetOptions options,
        GenomicsFactory.OfflineAuth auth, SexChromosomeFilter sexChromosomeFilter) throws IOException,
        GeneralSecurityException {
      String datasetId = options.getDatasetId();
      Genomics genomics = auth.getGenomics(auth.getDefaultFactory());

      Iterable<Contig> contigs =
          options.isAllReferences() ? Contig.getContigsInVariantSet(genomics, datasetId, sexChromosomeFilter)
              : Contig.parseContigsFromCommandLine(options.getReferences());

      List<SearchVariantsRequest> requests = Lists.newArrayList();
      for (Contig contig : contigs) {
        for (Contig shard : contig.getShards(options.getBasesPerShard())) {
          LOG.info("Adding request with " + shard.referenceName + " " + shard.start + " to "
              + shard.end);
          SearchVariantsRequest request = shard.getVariantsRequest(datasetId);
          if (options.getPageSize() > 0) {
            request.setPageSize(options.getPageSize());
          }
          if (options.getMaxCalls() > 0) {
            request.setMaxCalls(options.getMaxCalls());
          }
          requests.add(request);
        }
      }
      return requests;
    }

    public static void validateOptions(GenomicsDatasetOptions options) {
      Preconditions.checkArgument(0 < options.getBinSize(), "binSize must be greater than zero");
      GenomicsOptions.Methods.validateOptions(options);
    }

  }

  @Description("The ID of the Google Genomics dataset this pipeline is accessing. "
      + "Defaults to 1000 Genomes.")
  @Default.String("10473108253681171589")
  String getDatasetId();

  void setDatasetId(String datasetId);

  @Description("Path of the file to which to write.")
  String getOutput();

  void setOutput(String output);

  @Description("By default, variants analyses will be run on BRCA1.  Pass this flag to run on all "
      + "references present in the dataset.  Note that certain jobs such as PCA and IBS "
      + "will automatically exclude X and Y chromosomes when this option is true.")
  boolean isAllReferences();

  void setAllReferences(boolean allReferences);

  @Description("Comma separated tuples of reference:start:end,... Defaults to " + Contig.BRCA1)
  @Default.String(Contig.BRCA1)
  String getReferences();

  void setReferences(String references);

  @Description("The maximum number of bases per shard.")
  @Default.Long(Contig.DEFAULT_NUMBER_OF_BASES_PER_SHARD)
  long getBasesPerShard();

  void setBasesPerShard(long basesPerShard);

  @Description("The maximum number of calls to return. However, at least one variant will always "
      + "be returned, even if it has more calls than this limit.  By default the response will be as "
      + "large as the Genomics API will allow.  Use this option only to reduce response sizes.")
  @Default.Integer(0)
  int getMaxCalls();

  void setMaxCalls(int maxCalls);

  @Description("If querying a dataset with non-variant segments (such as Complete Genomics data "
      + "or data in Genome VCF (gVCF) format), specify this flag so that the pipeline correctly "
      + "takes into account non-variant segment records that overlap variants within the dataset.")
  @Default.Boolean(false)
  boolean getHasNonVariantSegments();

  void setHasNonVariantSegments(boolean hasNonVariantSegments);

  @Description("Genomic window \"bin\" size to use for data containing non-variant segments when "
      + "joining those non-variant segment records with variant records.")
  @Default.Integer(1000)
  int getBinSize();

  void setBinSize(int binSize);
}
