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

import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.logging.Logger;

/**
 * A common options class for all pipelines that operate over a single dataset and write their
 * analysis to a file.
 */
public interface GenomicsDatasetOptions extends GenomicsOptions {
  public static class Methods {

    private static final Logger LOG = Logger.getLogger(GenomicsDatasetOptions.class.getName());

    // TODO: If needed, add getReadRequests method
    public static List<SearchVariantsRequest> getVariantRequests(GenomicsDatasetOptions options,
        GenomicsFactory.OfflineAuth auth, boolean excludeXY) throws IOException,
        GeneralSecurityException {
      String datasetId = options.getDatasetId();
      Genomics genomics = auth.getGenomics(auth.getDefaultFactory());

      Iterable<Contig> contigs =
          options.isAllContigs() ? Contig.getContigsInVariantSet(genomics, datasetId, excludeXY)
              : Contig.parseContigsFromCommandLine(options.getReferences());

      List<SearchVariantsRequest> requests = Lists.newArrayList();
      for (Contig contig : contigs) {
        for (Contig shard : contig.getShards()) {
          LOG.info("Adding request with " + shard.referenceName + " " + shard.start + " to "
              + shard.end);
          requests.add(shard.getVariantsRequest(datasetId));
        }
      }
      return requests;
    }

    public static void validateOptions(GenomicsDatasetOptions options) {
      int binSize = options.getBinSize();
      if (binSize <= 0) {
        throw new IllegalArgumentException("binSize must be greater than zero");
      }
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

  @Description("Comma separated tuples of reference:start:end,... Defaults to " + Contig.BRCA1)
  @Default.String(Contig.BRCA1)
  String getReferences();

  void setReferences(String references);

  @Description("If querying a dataset in Genome VCF (gVCF) format, specify this "
      + "flag so that the pipeline correctly takes into account reference-matching "
      + "block records which overlap variants within the dataset.")
  @Default.Boolean(false)
  boolean getIsGvcf();

  void setIsGvcf(boolean isGvcf);

  @Description("Genomic window \"bin\" size to use for gVCF data when joining block-records with variants.")
  @Default.Integer(1000)
  int getBinSize();

  void setBinSize(int binSize);
}
