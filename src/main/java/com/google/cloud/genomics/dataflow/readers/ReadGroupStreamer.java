/*
 * Copyright (C) 2015 Google Inc.
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
package com.google.cloud.genomics.dataflow.readers;

import java.util.Collections;
import java.util.List;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.cloud.genomics.utils.ShardUtils.SexChromosomeFilter;
import com.google.genomics.v1.Read;
import com.google.genomics.v1.StreamReadsRequest;

/**
 * PTransform from a potentially large number of ReadGroupSets to streaming reads via gRPC.
 * 
 * The sharding occurs as a stage of the pipeline, unlike the ReadStreamer PTransform
 * where the shards are passed in.  This is useful when the number of shards may
 * potentially be larger than Dataflow's pipeline creation request size limit.
 */
public class ReadGroupStreamer extends PTransform<PCollection<String>, PCollection<Read>> {
  protected final GenomicsFactory.OfflineAuth auth;
  protected final ShardBoundary.Requirement shardBoundary;
  protected final String fields;
  protected final SexChromosomeFilter sexChromosomeFilter;

  /**
   * Create a streamer that can appropriately shard a potentially large number of ReadGroupSets.
   * 
   * @param auth The OfflineAuth to use for the request.
   * @param shardBoundary The shard boundary semantics to enforce.
   * @param fields Which fields to include in a partial response or null for all.
   * @param sexChromosomeFilter An enum value indicating how sex chromosomes should be
   *        handled in the result.
   */
  public ReadGroupStreamer(GenomicsFactory.OfflineAuth auth, ShardBoundary.Requirement shardBoundary,
      String fields, SexChromosomeFilter sexChromosomeFilter) {
    this.auth = auth;
    this.shardBoundary = shardBoundary;
    this.fields = fields;
    this.sexChromosomeFilter = sexChromosomeFilter;
  }
  
  @Override
  public PCollection<Read> apply(PCollection<String> readGroupSetIds) {
    return readGroupSetIds.apply(ParDo.of(new CreateReadRequests()))
        .apply(new ReadStreamer(auth, ShardBoundary.Requirement.STRICT, null));
  }

  private class CreateReadRequests extends DoFn<String, StreamReadsRequest> {

    @Override
    public void processElement(DoFn<String, StreamReadsRequest>.ProcessContext c)
        throws Exception {
      GenomicsDatasetOptions options = c.getPipelineOptions().as(GenomicsDatasetOptions.class);
      String readGroupSetId = c.element();

      List<StreamReadsRequest> requests = null;
      if (options.isAllReferences()) {
        requests = ShardUtils.getReadRequests(readGroupSetId, sexChromosomeFilter, options.getBasesPerShard(), auth);
      } else {
        requests =
            ShardUtils.getReadRequests(Collections.singletonList(readGroupSetId), options.getReferences(), options.getBasesPerShard());
      }
      for(StreamReadsRequest request : requests) {
        c.output(request);
      }
    }
  }  
}

