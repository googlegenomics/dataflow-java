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
package com.google.cloud.genomics.dataflow.pipelines;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.functions.AlleleSimilarityCalculator;
import com.google.cloud.genomics.dataflow.functions.CallSimilarityCalculatorFactory;
import com.google.cloud.genomics.dataflow.functions.FormatIBSData;
import com.google.cloud.genomics.dataflow.functions.IBSCalculator;
import com.google.cloud.genomics.dataflow.functions.grpc.JoinNonVariantSegmentsWithVariants;
import com.google.cloud.genomics.dataflow.readers.VariantReader;
import com.google.cloud.genomics.dataflow.readers.VariantStreamer;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.IdentityByStateOptions;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.Variant;

/**
 * A pipeline that computes Identity by State (IBS) for each pair of individuals in a dataset.
 *
 * See http://googlegenomics.readthedocs.org/en/latest/use_cases/compute_identity_by_state/index.html
 * for running instructions.
 */

public class IdentityByState {
  // TODO: https://github.com/googlegenomics/utils-java/issues/48
  private static final String VARIANT_FIELDS = "variants(start,calls(genotype,callSetName))";

  public static void main(String[] args) throws IOException, GeneralSecurityException,
      InstantiationException, IllegalAccessException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(IdentityByStateOptions.class);
    IdentityByStateOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IdentityByStateOptions.class);
    // Option validation is not yet automatic, we make an explicit call here.
    GenomicsDatasetOptions.Methods.validateOptions(options);

    OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    List<StreamVariantsRequest> requests = options.isAllReferences() ?
        ShardUtils.getVariantRequests(options.getDatasetId(), ShardUtils.SexChromosomeFilter.EXCLUDE_XY,
            options.getBasesPerShard(), auth) :
              ShardUtils.getVariantRequests(options.getDatasetId(), options.getReferences(), options.getBasesPerShard());

    Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);
    PCollection<Variant> variants = p.begin()
        .apply(Create.of(requests))
        .apply(new VariantStreamer(auth, ShardBoundary.Requirement.STRICT, VARIANT_FIELDS));

    PCollection<Variant> processedVariants;
    if(options.getHasNonVariantSegments()) {
        // Special handling is needed for data with non-variant segment records since IBS must
        // take into account reference-matches in addition to the variants (unlike
        // other analyses such as PCA).
      processedVariants = JoinNonVariantSegmentsWithVariants.joinVariantsTransform(variants);
    } else {
      processedVariants = variants;
    }

    processedVariants
        .apply(
            ParDo.named(AlleleSimilarityCalculator.class.getSimpleName()).of(
                new AlleleSimilarityCalculator(getCallSimilarityCalculatorFactory(options))))
        .apply(Combine.<KV<String, String>, KV<Double, Integer>>perKey(new IBSCalculator()))
        .apply(ParDo.named(FormatIBSData.class.getSimpleName()).of(new FormatIBSData()))
        .apply(TextIO.Write.named("WriteIBSData").to(options.getOutput()));

    p.run();
  }

  private static CallSimilarityCalculatorFactory getCallSimilarityCalculatorFactory(
      IdentityByStateOptions options) throws InstantiationException, IllegalAccessException {
    return options.getCallSimilarityCalculatorFactory().newInstance();
  }

}
