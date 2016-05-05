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

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.cloud.genomics.utils.ShardUtils.SexChromosomeFilter;
import com.google.genomics.v1.Read;
import com.google.genomics.v1.StreamReadsRequest;

import java.util.Collections;
import java.util.List;

/**
 * PTransform from a potentially large number of ReadGroupSets to streaming reads via gRPC.
 *
 * The sharding occurs as a stage of the pipeline, unlike the ReadStreamer PTransform
 * where the shards are passed in.  This is useful when the number of shards may
 * potentially be larger than Dataflow's pipeline creation request size limit.
 */
public class ReadGroupStreamer extends PTransform<PCollection<String>, PCollection<Read>> {
  protected final OfflineAuth auth;
  protected final ShardBoundary.Requirement shardBoundary;
  protected final String fields;
  protected final SexChromosomeFilter sexChromosomeFilter;

  /**
   * Create a streamer that can appropriately shard a potentially large number of ReadGroupSets.
   *
   * Tip: Use the API explorer to test which fields to include in partial responses:
   * <a href="https://developers.google.com/apis-explorer/#p/genomics/v1/genomics.reads.stream?fields=alignments(alignedSequence%252Cid)&_h=2&resource=%257B%250A++%2522readGroupSetId%2522%253A+%2522CMvnhpKTFhD3he72j4KZuyc%2522%252C%250A++%2522referenceName%2522%253A+%2522chr17%2522%252C%250A++%2522start%2522%253A+%252241196311%2522%252C%250A++%2522end%2522%253A+%252241196312%2522%250A%257D&">
   * reads example</a>.
   *
   * @param auth The OfflineAuth to use for the request.
   * @param shardBoundary The shard boundary semantics to enforce.
   * @param fields Which fields to include in a partial response or null for all.
   * @param sexChromosomeFilter An enum value indicating how sex chromosomes should be
   *        handled in the result.
   */
  public ReadGroupStreamer(OfflineAuth auth, ShardBoundary.Requirement shardBoundary,
      String fields, SexChromosomeFilter sexChromosomeFilter) {
    this.auth = auth;
    this.shardBoundary = shardBoundary;
    this.fields = fields;
    this.sexChromosomeFilter = sexChromosomeFilter;
  }

  @Override
  public PCollection<Read> apply(PCollection<String> readGroupSetIds) {
    return readGroupSetIds.apply(ParDo.of(new CreateReadRequests()))
        // Force a shuffle operation here to break the fusion of these steps.
        // By breaking fusion, the work will be distributed to all available workers.
        .apply(GroupByKey.<Integer, StreamReadsRequest>create())
        .apply(ParDo.of(new ConvergeStreamReadsRequestList()))
        .apply(new ReadStreamer(auth, ShardBoundary.Requirement.STRICT, fields));
  }

  private class CreateReadRequests extends DoFn<String, KV<Integer, StreamReadsRequest>> {

    @Override
    public void processElement(DoFn<String, KV<Integer, StreamReadsRequest>>.ProcessContext c)
        throws Exception {
      ShardOptions options = c.getPipelineOptions().as(ShardOptions.class);
      String readGroupSetId = c.element();

      List<StreamReadsRequest> requests = null;
      if (options.isAllReferences()) {
        requests = ShardUtils.getReadRequests(readGroupSetId, sexChromosomeFilter, options.getBasesPerShard(), auth);
      } else {
        requests =
            ShardUtils.getReadRequests(Collections.singletonList(readGroupSetId), options.getReferences(), options.getBasesPerShard());
      }
      for(StreamReadsRequest request : requests) {
        c.output(KV.of(request.hashCode(), request));
      }
    }
  }

  private class ConvergeStreamReadsRequestList extends DoFn<KV<Integer, Iterable<StreamReadsRequest>>, StreamReadsRequest> {
    @Override
    public void processElement(ProcessContext c) {
      for (StreamReadsRequest r : c.element().getValue()) {
        c.output(r);
      }
    }
  }
}

