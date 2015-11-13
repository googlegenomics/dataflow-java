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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Max;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.grpc.ReadStreamIterator;
import com.google.common.base.Stopwatch;
import com.google.genomics.v1.Read;
import com.google.genomics.v1.StreamReadsRequest;
import com.google.genomics.v1.StreamReadsResponse;

/**
 * PTransform for streaming reads via gRPC.
 */
public class ReadStreamer extends
PTransform<PCollection<StreamReadsRequest>, PCollection<Read>> {

  protected final GenomicsFactory.OfflineAuth auth;
  protected final ShardBoundary.Requirement shardBoundary;
  protected final String fields;

  /**
   * Create a streamer that can enforce shard boundary semantics.
   * 
   * @param auth The OfflineAuth to use for the request.
   * @param shardBoundary The shard boundary semantics to enforce.
   * @param fields Which fields to include in a partial response or null for all.
   */
  public ReadStreamer(GenomicsFactory.OfflineAuth auth, ShardBoundary.Requirement shardBoundary, String fields) {
    this.auth = auth;
    this.shardBoundary = shardBoundary;
    this.fields = fields;
  }

  @Override
  public PCollection<Read> apply(PCollection<StreamReadsRequest> input) {
    return input.apply(ParDo.of(new RetrieveReads()))
        .apply(ParDo.of(new ConvergeReadsList()));
  }


  private class RetrieveReads extends DoFn<StreamReadsRequest, List<Read>> {

    protected Aggregator<Integer, Integer> initializedShardCount;
    protected Aggregator<Integer, Integer> finishedShardCount;
    protected Aggregator<Long, Long> shardTimeMaxSec;

    public RetrieveReads() {
      initializedShardCount = createAggregator("Initialized Shard Count", new Sum.SumIntegerFn());
      finishedShardCount = createAggregator("Finished Shard Count", new Sum.SumIntegerFn());
      shardTimeMaxSec = createAggregator("Maximum Shard Processing Time (sec)", new Max.MaxLongFn());
    }

    @Override
    public void processElement(ProcessContext c) throws IOException, GeneralSecurityException {
      initializedShardCount.addValue(1);
      shardTimeMaxSec.addValue(0L);
      Stopwatch stopWatch = Stopwatch.createStarted();
      Iterator<StreamReadsResponse> iter = ReadStreamIterator.enforceShardBoundary(auth, c.element(), shardBoundary, fields);
      while (iter.hasNext()) {
        StreamReadsResponse readResponse = iter.next();
        c.output(readResponse.getAlignmentsList());
      }
      stopWatch.stop();
      shardTimeMaxSec.addValue(stopWatch.elapsed(TimeUnit.SECONDS));
      finishedShardCount.addValue(1);
    }
  }

  /**
   * This step exists to emit the individual reads in a parallel step to the StreamReads step in
   * order to increase throughput.
   */
  private class ConvergeReadsList extends DoFn<List<Read>, Read> {

    protected Aggregator<Long, Long> itemCount;

    public ConvergeReadsList() {
      itemCount = createAggregator("Number of reads", new Sum.SumLongFn());
    }

    @Override
    public void processElement(ProcessContext c) {
      for (Read r : c.element()) {
        c.output(r);
        itemCount.addValue(1L);
      }
    }
  }
}

