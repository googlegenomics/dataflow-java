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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.functions.JoinNonVariantSegmentsWithVariants;
import com.google.cloud.genomics.dataflow.functions.SitesToShards;
import com.google.cloud.genomics.dataflow.functions.VariantFunctions;
import com.google.cloud.genomics.dataflow.functions.ibs.AlleleSimilarityCalculator;
import com.google.cloud.genomics.dataflow.functions.ibs.CallSimilarityCalculatorFactory;
import com.google.cloud.genomics.dataflow.functions.ibs.FormatIBSData;
import com.google.cloud.genomics.dataflow.functions.ibs.IBSCalculator;
import com.google.cloud.genomics.dataflow.functions.ibs.SharedMinorAllelesCalculatorFactory;
import com.google.cloud.genomics.dataflow.readers.VariantStreamer;
import com.google.cloud.genomics.dataflow.utils.GCSOutputOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.Variant;

import java.util.List;

/**
 * A pipeline that computes Identity by State (IBS) for each pair of individuals in a dataset.
 *
 * See http://googlegenomics.readthedocs.org/en/latest/use_cases/compute_identity_by_state/index.html
 * for running instructions.
 */

public class IdentityByState {

  public static interface Options extends
    // Options for calculating over regions, chromosomes, or whole genomes.
    ShardOptions,
    // Options for calculating over a list of sites.
    SitesToShards.Options,
    // Options for special handling of data with non-variant segment records.  This
    // is needed since IBS must take into account reference-matches in addition
    // to the variants (unlike other analyses such as PCA).
    JoinNonVariantSegmentsWithVariants.Options,
    // Options for the output destination.
    GCSOutputOptions
    {

    @Description("The ID of the Google Genomics variant set this pipeline is accessing. "
        + "Defaults to 1000 Genomes.")
    @Default.String("10473108253681171589")
    String getVariantSetId();

    void setVariantSetId(String variantSetId);

    @Description("The class that determines the strategy for calculating the similarity of alleles.")
    @Default.Class(SharedMinorAllelesCalculatorFactory.class)
    Class<? extends CallSimilarityCalculatorFactory> getCallSimilarityCalculatorFactory();

    void setCallSimilarityCalculatorFactory(Class<? extends CallSimilarityCalculatorFactory> kls);

    public static class Methods {
      public static void validateOptions(Options options) {
        JoinNonVariantSegmentsWithVariants.Options.Methods.validateOptions(options);
        GCSOutputOptions.Methods.validateOptions(options);
      }
    }

  }

  // Tip: Use the API explorer to test which fields to include in partial responses.
  // https://developers.google.com/apis-explorer/#p/genomics/v1/genomics.variants.stream?fields=variants(alternateBases%252Ccalls(callSetName%252Cgenotype)%252CreferenceBases)&_h=3&resource=%257B%250A++%2522variantSetId%2522%253A+%25223049512673186936334%2522%252C%250A++%2522referenceName%2522%253A+%2522chr17%2522%252C%250A++%2522start%2522%253A+%252241196311%2522%252C%250A++%2522end%2522%253A+%252241196312%2522%252C%250A++%2522callSetIds%2522%253A+%250A++%255B%25223049512673186936334-0%2522%250A++%255D%250A%257D&
  private static final String VARIANT_FIELDS =
      "variants(alternateBases,calls(callSetName,genotype),end,referenceBases,start)";

  public static void main(String[] args) throws Exception {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(Options.class);
    Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    Options.Methods.validateOptions(options);
    OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);

    Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);
    PCollection<Variant> processedVariants = null;

    if(null != options.getSitesFilepath()) {
      // Compute IBS on a list of sites (e.g., SNPs).
      PCollection<StreamVariantsRequest> requests = p.apply(TextIO.Read.named("ReadSites")
          .from(options.getSitesFilepath()))
          .apply(new SitesToShards.SitesToStreamVariantsShardsTransform(options.getVariantSetId()));

      if(options.getHasNonVariantSegments()) {
        processedVariants = requests.apply(
            new JoinNonVariantSegmentsWithVariants.RetrieveAndCombineTransform(auth, VARIANT_FIELDS));
      } else {
        processedVariants = requests.apply(
            new VariantStreamer(auth, ShardBoundary.Requirement.NON_VARIANT_OVERLAPS, VARIANT_FIELDS));
      }
    } else {
      // Computing IBS over genomic region(s) or the whole genome.
      List<StreamVariantsRequest> requests = options.isAllReferences() ?
          ShardUtils.getVariantRequests(options.getVariantSetId(), ShardUtils.SexChromosomeFilter.EXCLUDE_XY,
              options.getBasesPerShard(), auth) :
                ShardUtils.getVariantRequests(options.getVariantSetId(), options.getReferences(), options.getBasesPerShard());
          PCollection<Variant> variants = p.begin()
              .apply(Create.of(requests))
              .apply(new VariantStreamer(auth, ShardBoundary.Requirement.STRICT, VARIANT_FIELDS));

          if(options.getHasNonVariantSegments()) {
            // Note that this is less exact compared to the above approach on sites.
            // When not run on a whole chromosome or genome, any non-variant segments at the beginning of the region(s)
            // are not considered due to the STRICT shard boundary used to avoid repeated data.
            processedVariants = variants.apply(new JoinNonVariantSegmentsWithVariants.BinShuffleAndCombineTransform());
          } else {
            processedVariants = variants;
          }
    }

    processedVariants
        .apply(Filter.byPredicate(VariantFunctions.IS_SINGLE_ALTERNATE_SNP))
        .apply(
            ParDo.named(AlleleSimilarityCalculator.class.getSimpleName()).of(
                new AlleleSimilarityCalculator(getCallSimilarityCalculatorFactory(options))))
        .apply(Combine.<KV<String, String>, KV<Double, Integer>>perKey(new IBSCalculator()))
        .apply(ParDo.named(FormatIBSData.class.getSimpleName()).of(new FormatIBSData()))
        .apply(TextIO.Write.named("WriteIBSData").to(options.getOutput()));

    p.run();
  }

  private static CallSimilarityCalculatorFactory getCallSimilarityCalculatorFactory(
      Options options) throws InstantiationException, IllegalAccessException {
    return options.getCallSimilarityCalculatorFactory().newInstance();
  }
}
