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
package com.google.cloud.genomics.dataflow.utils;

import com.google.api.client.json.GenericJson;
import com.google.api.services.genomics.model.Beacon;
import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.CallSet;
import com.google.api.services.genomics.model.CoverageBucket;
import com.google.api.services.genomics.model.ExperimentalCreateJobRequest;
import com.google.api.services.genomics.model.ExperimentalCreateJobResponse;
import com.google.api.services.genomics.model.ExportReadsetsRequest;
import com.google.api.services.genomics.model.ExportReadsetsResponse;
import com.google.api.services.genomics.model.ExportVariantsRequest;
import com.google.api.services.genomics.model.ExportVariantsResponse;
import com.google.api.services.genomics.model.GenomicRange;
import com.google.api.services.genomics.model.Header;
import com.google.api.services.genomics.model.HeaderSection;
import com.google.api.services.genomics.model.ImportReadsetsRequest;
import com.google.api.services.genomics.model.ImportReadsetsResponse;
import com.google.api.services.genomics.model.ImportVariantsRequest;
import com.google.api.services.genomics.model.ImportVariantsResponse;
import com.google.api.services.genomics.model.JobRequest;
import com.google.api.services.genomics.model.ListCoverageBucketsResponse;
import com.google.api.services.genomics.model.ListDatasetsResponse;
import com.google.api.services.genomics.model.Program;
import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.ReadGroup;
import com.google.api.services.genomics.model.Readset;
import com.google.api.services.genomics.model.ReferenceBound;
import com.google.api.services.genomics.model.ReferenceSequence;
import com.google.api.services.genomics.model.SearchCallSetsRequest;
import com.google.api.services.genomics.model.SearchCallSetsResponse;
import com.google.api.services.genomics.model.SearchJobsRequest;
import com.google.api.services.genomics.model.SearchJobsResponse;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.api.services.genomics.model.SearchReadsResponse;
import com.google.api.services.genomics.model.SearchReadsetsRequest;
import com.google.api.services.genomics.model.SearchReadsetsResponse;
import com.google.api.services.genomics.model.SearchVariantSetsRequest;
import com.google.api.services.genomics.model.SearchVariantSetsResponse;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.api.services.genomics.model.SearchVariantsResponse;
import com.google.api.services.genomics.model.Variant;
import com.google.api.services.genomics.model.VariantSet;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;

import java.util.Arrays;
import java.util.List;

public class PipelineFactory {

  public static Pipeline create() {
    Pipeline pipeline = Pipeline.create();
    CoderRegistry registry = pipeline.getCoderRegistry();
    for (final Class<? extends GenericJson> type : Arrays.asList(
        Beacon.class,
        Call.class,
        CallSet.class,
        CoverageBucket.class,
        ExperimentalCreateJobRequest.class,
        ExperimentalCreateJobResponse.class,
        ExportReadsetsRequest.class,
        ExportReadsetsResponse.class,
        ExportVariantsRequest.class,
        ExportVariantsResponse.class,
        GenomicRange.class,
        Header.class,
        HeaderSection.class,
        ImportReadsetsRequest.class,
        ImportReadsetsResponse.class,
        ImportVariantsRequest.class,
        ImportVariantsResponse.class,
        JobRequest.class,
        ListCoverageBucketsResponse.class,
        ListDatasetsResponse.class,
        Program.class,
        Read.class,
        ReadGroup.class,
        Readset.class,
        ReferenceBound.class,
        ReferenceSequence.class,
        SearchCallSetsRequest.class,
        SearchCallSetsResponse.class,
        SearchJobsRequest.class,
        SearchJobsResponse.class,
        SearchReadsRequest.class,
        SearchReadsResponse.class,
        SearchReadsetsRequest.class,
        SearchReadsetsResponse.class,
        SearchVariantSetsRequest.class,
        SearchVariantSetsResponse.class,
        SearchVariantsRequest.class,
        SearchVariantsResponse.class,
        Variant.class,
        VariantSet.class)) {
      registry.registerCoder(type,
          new CoderRegistry.CoderFactory() {
            @Override public Coder<?> create(
                @SuppressWarnings("rawtypes") List<? extends Coder> unused) {
              return GenericJsonCoder.of(type);
            }
          });
    }
    return pipeline;
  }
}
