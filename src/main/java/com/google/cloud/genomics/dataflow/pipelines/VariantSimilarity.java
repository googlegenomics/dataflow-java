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
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.utils.OptionsParser;
import com.google.cloud.genomics.dataflow.functions.ExtractSimilarCallsets;
import com.google.cloud.genomics.dataflow.functions.OutputPCoAFile;
import com.google.cloud.genomics.dataflow.readers.VariantReader;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsAuth;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

/**
 * An pipeline that generates similarity data for variants in a dataset.
 * See the README for running instructions.
 */
public class VariantSimilarity {
  private static final String VARIANT_FIELDS
      = "nextPageToken,variants(id,calls(genotype,callSetName))";

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    GenomicsDatasetOptions options = OptionsParser.parse(args, GenomicsDatasetOptions.class,
        VariantSimilarity.class.getSimpleName());
    GenomicsOptions.Methods.validateOptions(options);

    GenomicsAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    List<SearchVariantsRequest> requests = GenomicsDatasetOptions.Methods.getVariantRequests(options, auth);

    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);
    DataflowWorkarounds.getPCollection(requests, p, options.getNumWorkers())
        .apply(ParDo.named("VariantReader")
            .of(new VariantReader(auth, VARIANT_FIELDS)))
        .apply(ParDo.named("ExtractSimilarCallsets").of(new ExtractSimilarCallsets()))
        .apply(new OutputPCoAFile(options.getOutput()));

    p.run();
  }
}
