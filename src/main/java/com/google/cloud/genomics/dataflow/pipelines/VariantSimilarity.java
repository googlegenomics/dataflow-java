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

import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.genomics.dataflow.functions.ExtractSimilarCallsets;
import com.google.cloud.genomics.dataflow.functions.OutputPCoAFile;
import com.google.cloud.genomics.dataflow.readers.VariantReader;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.Contig.SexChromosomeFilter;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.Paginator.ShardBoundary;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

/**
 * A pipeline that generates similarity data for variants in a dataset.
 * See the README for running instructions.
 */
public class VariantSimilarity {
  private static final String VARIANT_FIELDS
      = "nextPageToken,variants(start,calls(genotype,callSetName))";

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(GenomicsDatasetOptions.class);
    GenomicsDatasetOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(GenomicsDatasetOptions.class);
    // Option validation is not yet automatic, we make an explicit call here.
    GenomicsDatasetOptions.Methods.validateOptions(options);

    GenomicsFactory.OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    List<SearchVariantsRequest> requests =
        GenomicsDatasetOptions.Methods.getVariantRequests(options, auth,
            SexChromosomeFilter.EXCLUDE_XY);

    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);
    p.begin()
        .apply(Create.of(requests))
        .apply(
            ParDo.named("VariantReader").of(
                new VariantReader(auth, ShardBoundary.STRICT, VARIANT_FIELDS)))
        .apply(ParDo.named("ExtractSimilarCallsets").of(new ExtractSimilarCallsets()))
        .apply(new OutputPCoAFile(options.getOutput()));

    p.run();
  }
}
