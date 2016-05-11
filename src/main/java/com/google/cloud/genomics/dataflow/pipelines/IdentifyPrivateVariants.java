/*
 * Copyright (C) 2016 Google Inc.
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
import com.google.cloud.dataflow.sdk.options.Validation.Required;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.readers.VariantStreamer;
import com.google.cloud.genomics.dataflow.utils.GCSOutputOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.Variant;
import com.google.genomics.v1.VariantCall;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.util.List;

/**
 * Given a list of callset ids, identify variants that are associated only with the specified
 * individuals (i.e. variants private to those individuals).
 *
 * This pipeline might be used in combination with the DeleteVariants pipeline to, for example,
 * remove all variants private to a particular family from the variant set.
 */
public class IdentifyPrivateVariants {

  public static interface Options extends
  // Options for calculating over regions, chromosomes, or whole genomes.
  ShardOptions,
  // Options for the output destination.
  GCSOutputOptions {

    @Description("The ID of the Google Genomics variant set from which this pipeline "
        + "will identify private variants.")
    @Required
    String getVariantSetId();
    void setVariantSetId(String variantSetId);

    @Description("A local file path to a list of newline-separated callset IDs. "
        + "Any variants private to those callset IDs will be deleted.")
    @Required
    String getCallSetIdsFilepath();
    void setCallSetIdsFilepath(String filepath);

    @Description("Whether variants with no callsets should also be identified.  Defaults to false.")
    @Default.Boolean(false)
    boolean getIdentifyVariantsWithoutCalls();
    void setIdentifyVariantsWithoutCalls(boolean identifyVariantsWithoutCalls);

    public static class Methods {
      public static void validateOptions(Options options) {
        GCSOutputOptions.Methods.validateOptions(options);
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(IdentifyPrivateVariants.class);
  // Tip: Use the API explorer to test which fields to include in partial responses.
  // https://developers.google.com/apis-explorer/#p/genomics/v1/genomics.variants.stream?fields=variants(alternateBases%252Ccalls(callSetName%252Cgenotype)%252CreferenceBases)&_h=3&resource=%257B%250A++%2522variantSetId%2522%253A+%25223049512673186936334%2522%252C%250A++%2522referenceName%2522%253A+%2522chr17%2522%252C%250A++%2522start%2522%253A+%252241196311%2522%252C%250A++%2522end%2522%253A+%252241196312%2522%252C%250A++%2522callSetIds%2522%253A+%250A++%255B%25223049512673186936334-0%2522%250A++%255D%250A%257D&
  private static final String VARIANT_FIELDS = "variants(id,reference_name,start,end,reference_bases,alternate_bases,calls(callSetId))";

  /**
   * Pipeline function implementing a filter only returning variants private to one or more callset
   * IDs and optionally those with no callsetIds.
   */
  public static final class PrivateVariantsFilterFn extends DoFn<Variant, Variant> {

    private final ImmutableSet<String> callSetIds;
    private boolean retainVariantsWithNoCalls;

    /**
     * @param callSetIds
     */
    public PrivateVariantsFilterFn(ImmutableSet<String> callSetIds,
        boolean retainVariantsWithNoCalls) {
      super();
      this.callSetIds = callSetIds;
      this.retainVariantsWithNoCalls = retainVariantsWithNoCalls;
    }

    @Override
    public void processElement(ProcessContext context) {
      Variant variant = context.element();
      List<VariantCall> calls = variant.getCallsList();

      for (VariantCall call : calls) {
        if (!callSetIds.contains(call.getCallSetId())) {
          // We found a callset ID not in our set. This variant is not private
          // to our set of callset IDs. Skip it.
          return;
        }
      }

      if (!retainVariantsWithNoCalls && calls.isEmpty()) {
        // This is a variant with no calls.  Skip it.
        return;
      }

      context.output(variant);
    }
  }

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(Options.class);
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    Options.Methods.validateOptions(options);

    OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);

    // Grab and parse the list of callset IDs.
    String fileContents =
        Files.toString(new File(options.getCallSetIdsFilepath()), Charset.defaultCharset());
    ImmutableSet<String> callSetIds =
        ImmutableSet
            .<String>builder()
            .addAll(
                Splitter.on(CharMatcher.BREAKING_WHITESPACE).omitEmptyStrings().trimResults()
                    .split(fileContents)).build();
    LOG.info("The pipeline will identify and write to Cloud Storage variants "
        + "private to " + callSetIds.size() + " genomes with callSetIds: " + callSetIds);
    if (options.getIdentifyVariantsWithoutCalls()) {
      LOG.info("* The pipeline will also identify variants with no callsets. *");
    }

    List<StreamVariantsRequest> shardRequests =
        options.isAllReferences() ? ShardUtils.getVariantRequests(options.getVariantSetId(),
            ShardUtils.SexChromosomeFilter.INCLUDE_XY, options.getBasesPerShard(), auth)
            : ShardUtils.getVariantRequests(options.getVariantSetId(), options.getReferences(),
                options.getBasesPerShard());

    Pipeline p = Pipeline.create(options);
    PCollection<Variant> variants = p.begin()
        .apply(Create.of(shardRequests))
        .apply(new VariantStreamer(auth, ShardBoundary.Requirement.STRICT, VARIANT_FIELDS))
        .apply(ParDo.of(new PrivateVariantsFilterFn(callSetIds,
            options.getIdentifyVariantsWithoutCalls())));

    variants.apply(ParDo.named("FormatResults").of(new DoFn<Variant, String>() {
      @Override
      public void processElement(ProcessContext c) {
        Variant v = c.element();
        c.output(Joiner.on("\t").join(v.getId(),
            v.getReferenceName(),
            v.getStart(),
            v.getEnd(),
            v.getReferenceBases(),
            Joiner.on(",").join(v.getAlternateBasesList())
            ));
      }
    }))
    .apply(TextIO.Write.to(options.getOutput()));

    p.run();
  }
}
