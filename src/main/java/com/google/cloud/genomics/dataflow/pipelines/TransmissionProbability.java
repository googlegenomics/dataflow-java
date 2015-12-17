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
import java.util.List;

import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.functions.CalculateTransmissionProbability;
import com.google.cloud.genomics.dataflow.functions.ExtractAlleleTransmissionStatus;
import com.google.cloud.genomics.dataflow.model.Allele;
import com.google.cloud.genomics.dataflow.readers.VariantReader;
import com.google.cloud.genomics.dataflow.utils.GCSOutputOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;

/**
 * A pipeline that calculates the transmission probability of each allele in parents.
 * See the README for running instructions.
 */
public class TransmissionProbability {

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
  private static final String VARIANT_FIELDS
      = "nextPageToken,variants(id,start,names,calls(info,callSetName))";

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(Options.class);
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    Options.Methods.validateOptions(options);

    OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    List<SearchVariantsRequest> requests = options.isAllReferences() ?
        ShardUtils.getPaginatedVariantRequests(options.getVariantSetId(), ShardUtils.SexChromosomeFilter.EXCLUDE_XY,
            options.getBasesPerShard(), auth) :
              ShardUtils.getPaginatedVariantRequests(options.getVariantSetId(), options.getReferences(), options.getBasesPerShard());

    Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);

    // The below pipeline works as follows:
    //    - Fetch the variants,
    //    - For each Variant, see which alleles were actually transmitted to
    //        the child
    //    - Groups Transmission sources by Variant,
    //    - Calculate transmission Probability for each variant
    //    - Print calculated values to a file.
    p.begin()
        .apply(Create.of(requests))
        .apply(ParDo.named("VariantReader")
            .of(new VariantReader(auth, ShardBoundary.Requirement.STRICT, VARIANT_FIELDS)))
        .apply(ParDo.named("ExtractFamilyVariantStatus")
            .of(new ExtractAlleleTransmissionStatus()))
        .apply(GroupByKey.<Allele, Boolean>create())
        .apply(ParDo.named("CalculateTransmissionProbability")
            .of(new CalculateTransmissionProbability()))
        .apply(ParDo.named("WriteDataToFile")
            .of(new DoFn<KV<Allele, Double>, String>() {
          @Override
          public void processElement(ProcessContext c) {
            KV<Allele, Double> pair = c.element();
            String output = pair.getKey().getReferenceName() + "\t" +
                pair.getKey().getAllele() + "\t" + pair.getValue() + "\n";
            c.output(output);
          }
        }))
        .apply(TextIO.Write.named("WriteResults").to(options.getOutput()));

    p.run();
  }
}
