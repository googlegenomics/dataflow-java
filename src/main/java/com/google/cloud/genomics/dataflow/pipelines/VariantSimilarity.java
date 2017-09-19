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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.functions.SitesToShards;
import com.google.cloud.genomics.dataflow.functions.pca.ExtractSimilarCallsets;
import com.google.cloud.genomics.dataflow.functions.pca.OutputPCoAFile;
import com.google.cloud.genomics.dataflow.readers.VariantStreamer;
import com.google.cloud.genomics.dataflow.utils.CallSetNamesOptions;
import com.google.cloud.genomics.dataflow.utils.GCSOutputOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.GenomicsUtils;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.genomics.v1.StreamVariantsRequest;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;

/**
 * A pipeline that generates similarity data for variants in a dataset.
 *
 * See http://googlegenomics.readthedocs.org/en/latest/use_cases/compute_principal_coordinate_analysis/index.html
 * for running instructions.
 */
public class VariantSimilarity {

  public static interface Options extends
    // Options for call set names.
    CallSetNamesOptions,
    // Options for calculating over regions, chromosomes, or whole genomes.
    ShardOptions,
    // Options for calculating over a list of sites.
    SitesToShards.Options,
    // Options for the output destination.
    GCSOutputOptions {

    @Description("Whether to wait until the pipeline completes. This is useful "
      + "for test purposes.")
    @Default.Boolean(false)
    boolean getWait();
    void setWait(boolean wait);

    public static class Methods {
      public static void validateOptions(Options options) {
        GCSOutputOptions.Methods.validateOptions(options);
      }
    }
  }

  // Tip: Use the API explorer to test which fields to include in partial responses.
  // https://developers.google.com/apis-explorer/#p/genomics/v1/genomics.variants.stream?fields=variants(alternateBases%252Ccalls(callSetName%252Cgenotype)%252CreferenceBases)&_h=3&resource=%257B%250A++%2522variantSetId%2522%253A+%25223049512673186936334%2522%252C%250A++%2522referenceName%2522%253A+%2522chr17%2522%252C%250A++%2522start%2522%253A+%252241196311%2522%252C%250A++%2522end%2522%253A+%252241196312%2522%252C%250A++%2522callSetIds%2522%253A+%250A++%255B%25223049512673186936334-0%2522%250A++%255D%250A%257D&
  private static final String VARIANT_FIELDS = "variants(start,calls(genotype,callSetName))";

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(Options.class);
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    Options.Methods.validateOptions(options);

    // Set up the prototype request and auth.
    StreamVariantsRequest prototype = CallSetNamesOptions.Methods.getRequestPrototype(options);
    OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);

    // Make a bimap of the callsets so that the indices the pipeline is passing around are small.
    List<String> callSetNames = (0 < prototype.getCallSetIdsCount())
        ? Lists.newArrayList(CallSetNamesOptions.Methods.getCallSetNames(options))
            : GenomicsUtils.getCallSetsNames(options.getVariantSetId(), auth);
    Collections.sort(callSetNames); // Ensure a stable sort order for reproducible results.
    BiMap<String, Integer> dataIndices = HashBiMap.create();
    for(String callSetName : callSetNames) {
      dataIndices.put(callSetName, dataIndices.size());
    }

    Pipeline p = Pipeline.create(options);
    p.begin();

    PCollection<StreamVariantsRequest> requests;
    if(null != options.getSitesFilepath()) {
      // Compute PCA on a list of sites.
      requests = p.apply("ReadSites", TextIO.read().from(options.getSitesFilepath()))
          .apply(new SitesToShards.SitesToStreamVariantsShardsTransform(prototype));
    } else {
      // Compute PCA over genomic regions.
      List<StreamVariantsRequest> shardRequests = options.isAllReferences() ?
          ShardUtils.getVariantRequests(prototype, ShardUtils.SexChromosomeFilter.EXCLUDE_XY,
              options.getBasesPerShard(), auth) :
            ShardUtils.getVariantRequests(prototype, options.getBasesPerShard(), options.getReferences());

      requests = p.apply(Create.of(shardRequests));
    }

    requests.apply(new VariantStreamer(auth, ShardBoundary.Requirement.STRICT, VARIANT_FIELDS))
        .apply(ParDo.of(new ExtractSimilarCallsets()))
        .apply(new OutputPCoAFile(dataIndices, options.getOutput()));

    PipelineResult result = p.run();
    if(options.getWait()) {
      result.waitUntilFinish();
    }
  }
}
