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
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.functions.ExtractSimilarCallsets;
import com.google.cloud.genomics.dataflow.functions.OutputPCoAFile;
import com.google.cloud.genomics.dataflow.readers.VariantReader;
import com.google.cloud.genomics.dataflow.readers.VariantStreamer;
import com.google.cloud.genomics.dataflow.utils.GCSOutputOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.GenomicsUtils;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.genomics.v1.StreamVariantsRequest;

/**
 * A pipeline that generates similarity data for variants in a dataset.
 *
 * See http://googlegenomics.readthedocs.org/en/latest/use_cases/compute_principal_coordinate_analysis/index.html
 * for running instructions.
 */
public class VariantSimilarity {

  public static interface Options extends ShardOptions, GCSOutputOptions {

    @Description("The ID of the Google Genomics variant set this pipeline is accessing. "
        + "Defaults to 1000 Genomes.")
    @Default.String("10473108253681171589")
    String getVariantSetId();

    void setVariantSetId(String variantSetId);

    @Description("Whether to use the gRPC API endpoint for variants.  Defaults to 'true';")
    @Default.Boolean(true)
    Boolean getUseGrpc();

    void setUseGrpc(Boolean value);

    public static class Methods {
      public static void validateOptions(Options options) {
        GCSOutputOptions.Methods.validateOptions(options);
      }
    }

  }

  // TODO https://github.com/googlegenomics/utils-java/issues/48
  private static final String VARIANT_FIELDS = "nextPageToken,variants(start,calls(genotype,callSetName))";
  
  public static void main(String[] args) throws IOException, GeneralSecurityException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(Options.class);
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    Options.Methods.validateOptions(options);

    OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);

    List<String> callSetNames = GenomicsUtils.getCallSetsNames(options.getVariantSetId() , auth);
    Collections.sort(callSetNames); // Ensure a stable sort order for reproducible results.
    BiMap<String, Integer> dataIndices = HashBiMap.create();
    for(String callSetName : callSetNames) {
      dataIndices.put(callSetName, dataIndices.size());
    }

    Pipeline p = Pipeline.create(options);

    p.begin();
    PCollection<KV<KV<String, String>, Long>> similarCallsets = null;

    if(options.getUseGrpc()) {
      List<StreamVariantsRequest> requests = options.isAllReferences() ?
          ShardUtils.getVariantRequests(options.getVariantSetId(), ShardUtils.SexChromosomeFilter.EXCLUDE_XY,
              options.getBasesPerShard(), auth) :
            ShardUtils.getVariantRequests(options.getVariantSetId(), options.getReferences(), options.getBasesPerShard());
      
      similarCallsets = p.apply(Create.of(requests))     
      .apply(new VariantStreamer(auth, ShardBoundary.Requirement.STRICT, VARIANT_FIELDS))
      .apply(ParDo.of(new ExtractSimilarCallsets.v1()));
    } else {
      p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);
      List<SearchVariantsRequest> requests = options.isAllReferences() ?
          ShardUtils.getPaginatedVariantRequests(options.getVariantSetId(), ShardUtils.SexChromosomeFilter.EXCLUDE_XY,
              options.getBasesPerShard(), auth) :
                ShardUtils.getPaginatedVariantRequests(options.getVariantSetId(), options.getReferences(), options.getBasesPerShard());

      similarCallsets = p.apply(Create.of(requests))
          .apply(ParDo.of(new VariantReader(auth, ShardBoundary.Requirement.STRICT, VARIANT_FIELDS)))
          .apply(ParDo.of(new ExtractSimilarCallsets.v1beta2()));
    }

    similarCallsets.apply(new OutputPCoAFile(dataIndices, options.getOutput()));

    p.run();
  }
}
