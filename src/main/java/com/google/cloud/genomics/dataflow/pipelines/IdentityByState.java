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
import com.google.cloud.dataflow.sdk.options.CliPipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.functions.FormatIBSData;
import com.google.cloud.genomics.dataflow.functions.IBSCalculator;
import com.google.cloud.genomics.dataflow.functions.SharedAllelesCounter;
import com.google.cloud.genomics.dataflow.readers.VariantReader;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;

/**
 * A pipeline that computes Identity by State (IBS) for each pair of individuals in a dataset.
 *
 * See the README for running instructions.
 */
public class IdentityByState {
  private static final String VARIANT_FIELDS = "nextPageToken,variants(id,calls(genotype,callSetName))";

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    GenomicsDatasetOptions options =
        CliPipelineOptionsFactory.create(GenomicsDatasetOptions.class, args);
    GenomicsOptions.Methods.validateOptions(options);

    GenomicsFactory.OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    List<SearchVariantsRequest> requests =
        GenomicsDatasetOptions.Methods.getVariantRequests(options, auth);

    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);
    DataflowWorkarounds
        .getPCollection(requests, p, options.getNumWorkers())
        .apply(
            ParDo.named(VariantReader.class.getSimpleName()).of(
                new VariantReader(auth, VARIANT_FIELDS)))
        .apply(
            ParDo.named(SharedAllelesCounter.class.getSimpleName()).of(new SharedAllelesCounter()))
        .apply(Combine.<KV<String, String>, KV<Double, Integer>>perKey(new IBSCalculator()))
        .apply(ParDo.named(FormatIBSData.class.getSimpleName()).of(new FormatIBSData()))
        .apply(TextIO.Write.named("WriteIBSData").to(options.getOutput()));

    p.run();
  }
}
