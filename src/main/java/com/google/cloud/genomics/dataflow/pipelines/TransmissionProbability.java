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
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.functions.CalculateTransmissionProbability;
import com.google.cloud.genomics.dataflow.functions.ExtractAlleleTransmissionStatus;
import com.google.cloud.genomics.dataflow.model.Allele;
import com.google.cloud.genomics.dataflow.readers.VariantReader;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.Paginator.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.cloud.genomics.utils.ShardUtils.SexChromosomeFilter;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

/**
 * A pipeline that calculates the transmission probability of each allele in parents.
 * See the README for running instructions.
 */
public class TransmissionProbability {
  private static final String VARIANT_FIELDS
      = "nextPageToken,variants(id,start,names,calls(info,callSetName))";

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(GenomicsDatasetOptions.class);
    GenomicsDatasetOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(GenomicsDatasetOptions.class);
    // Option validation is not yet automatic, we make an explicit call here.
    GenomicsDatasetOptions.Methods.validateOptions(options);

    GenomicsFactory.OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    List<SearchVariantsRequest> requests = options.isAllReferences() ?
        ShardUtils.getPaginatedVariantRequests(options.getDatasetId(), ShardUtils.SexChromosomeFilter.EXCLUDE_XY,
            options.getBasesPerShard(), auth) :
              ShardUtils.getPaginatedVariantRequests(options.getDatasetId(), options.getReferences(), options.getBasesPerShard());

    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);

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
            .of(new VariantReader(auth, ShardBoundary.STRICT, VARIANT_FIELDS)))
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
