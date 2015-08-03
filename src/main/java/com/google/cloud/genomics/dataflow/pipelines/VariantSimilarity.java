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

import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Proto2Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.functions.ExtractSimilarCallsets;
import com.google.cloud.genomics.dataflow.functions.OutputPCoAFile;
import com.google.cloud.genomics.dataflow.readers.VariantReader;
import com.google.cloud.genomics.dataflow.readers.VariantStreamer;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.GenomicsUtils;
import com.google.cloud.genomics.utils.Paginator.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.Variant;

/**
 * A pipeline that generates similarity data for variants in a dataset.
 *
 * See http://googlegenomics.readthedocs.org/en/latest/use_cases/compute_principal_coordinate_analysis/index.html
 * for running instructions.
 */
public class VariantSimilarity {
  private static final String VARIANT_FIELDS
    = "nextPageToken,variants(start,calls(genotype,callSetName))";

  public static interface VariantSimilarityOptions extends GenomicsDatasetOptions, GCSOptions {
    @Default.Boolean(true)
    Boolean getUseAlpn();

    void setUseAlpn(Boolean value);

    @Default.Boolean(false)
    Boolean getUseStreaming();

    void setUseStreaming(Boolean value);
  }

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(VariantSimilarityOptions.class);
    VariantSimilarityOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(VariantSimilarityOptions.class);
    // Option validation is not yet automatic, we make an explicit call here.
    GenomicsDatasetOptions.Methods.validateOptions(options);

    GenomicsFactory.OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);

    // Use integer indices instead of string callSetNames to reduce data sizes.
    List<String> callSetNames = GenomicsUtils.getCallSetsNames(options.getDatasetId() , auth);
    Collections.sort(callSetNames); // Ensure a stable sort order for reproducible results.
    BiMap<String, Integer> dataIndices = HashBiMap.create();
    for(String callSetName : callSetNames) {
      dataIndices.put(callSetName, dataIndices.size());
    }

    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);
    DataflowWorkarounds.registerCoder(p, Variant.class,
        SerializableCoder.of(Variant.class));
    DataflowWorkarounds.registerCoder(p, StreamVariantsRequest.class,
        Proto2Coder.of(StreamVariantsRequest.class));

    p.begin();
    PCollection<KV<KV<Integer, Integer>, Long>> similarCallsets = null;

    if(options.getUseStreaming()) {
      List<StreamVariantsRequest> requests = options.isAllReferences() ?
          ShardUtils.getVariantRequests(options.getDatasetId(), ShardUtils.SexChromosomeFilter.EXCLUDE_XY,
              options.getBasesPerShard(), auth) :
            ShardUtils.getVariantRequests(options.getDatasetId(), options.getReferences(), options.getBasesPerShard());
      
      similarCallsets = p.apply(Create.of(requests))
      .apply(new VariantStreamer.StreamVariants())
      .apply(ParDo.of(new ExtractSimilarCallsets.v1(dataIndices)));
    } else {
      List<SearchVariantsRequest> requests = options.isAllReferences() ?
          ShardUtils.getPaginatedVariantRequests(options.getDatasetId(), ShardUtils.SexChromosomeFilter.EXCLUDE_XY,
              options.getBasesPerShard(), auth) :
                ShardUtils.getPaginatedVariantRequests(options.getDatasetId(), options.getReferences(), options.getBasesPerShard());

      similarCallsets = p.apply(Create.of(requests))
          .apply(ParDo.of(new VariantReader(auth, ShardBoundary.STRICT, VARIANT_FIELDS)))
          .apply(ParDo.of(new ExtractSimilarCallsets.v1beta2(dataIndices)));
    }

    similarCallsets.apply(new OutputPCoAFile(dataIndices, options.getOutput()));

    p.run();
  }
}
