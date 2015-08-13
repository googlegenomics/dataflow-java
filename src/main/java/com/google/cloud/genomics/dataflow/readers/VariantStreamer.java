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

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Max;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.grpc.VariantStreamIterator;
import com.google.common.base.Stopwatch;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.StreamVariantsResponse;
import com.google.genomics.v1.Variant;

/**
 * PTransform for streaming variants via gRPC.
 */
public class VariantStreamer extends
PTransform<PCollection<StreamVariantsRequest>, PCollection<Variant>> {

  private static final Logger LOG = LoggerFactory.getLogger(VariantStreamer.class);
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
  public VariantStreamer(GenomicsFactory.OfflineAuth auth, ShardBoundary.Requirement shardBoundary, String fields) {
    this.auth = auth;
    this.shardBoundary = shardBoundary;
    this.fields = fields;
  }

  @Override
  public PCollection<Variant> apply(PCollection<StreamVariantsRequest> input) {
    return input.apply(ParDo.of(new RetrieveVariants()))
        .apply(ParDo.of(new ConvergeVariantsList()));
  }

  private class RetrieveVariants extends DoFn<StreamVariantsRequest, List<Variant>> {

    protected Aggregator<Integer, Integer> initializedShardCount;
    protected Aggregator<Integer, Integer> finishedShardCount;
    protected Aggregator<Long, Long> shardTimeMaxSec;
    DescriptiveStatistics stats;

    public RetrieveVariants() {
      initializedShardCount = createAggregator("Initialized Shard Count", new Sum.SumIntegerFn());
      finishedShardCount = createAggregator("Finished Shard Count", new Sum.SumIntegerFn());
      shardTimeMaxSec = createAggregator("Maximum Shard Processing Time (sec)", new Max.MaxLongFn());
      stats = new DescriptiveStatistics(500);
    }

    @Override
    public void processElement(ProcessContext c) throws IOException, GeneralSecurityException, InterruptedException {
      initializedShardCount.addValue(1);
      shardTimeMaxSec.addValue(0L);
      Stopwatch stopWatch = Stopwatch.createStarted();
      Iterator<StreamVariantsResponse> iter = new VariantStreamIterator(c.element(), auth, shardBoundary, fields);
      while (iter.hasNext()) {
        StreamVariantsResponse variantResponse = iter.next();
        c.output(variantResponse.getVariantsList());
      }
      stopWatch.stop();
      shardTimeMaxSec.addValue(stopWatch.elapsed(TimeUnit.SECONDS));
      stats.addValue(stopWatch.elapsed(TimeUnit.SECONDS));
      finishedShardCount.addValue(1);
      LOG.info("Shard Duration in Seconds - Min: " + stats.getMin() + " Max: " + stats.getMax() +
          " Avg: " + stats.getMean() + " StdDev: " + stats.getStandardDeviation());      
    }
  }
  
  /**
   * This step exists to emit the individual variants in a parallel step to the StreamVariants step
   * in order to increase throughput.
   */
  private class ConvergeVariantsList extends DoFn<List<Variant>, Variant> {

    protected Aggregator<Long, Long> itemCount;

    public ConvergeVariantsList() {
      itemCount = createAggregator("Number of variants", new Sum.SumLongFn());
    }

    @Override
    public void processElement(ProcessContext c) {
      for (Variant v : c.element()) {
        c.output(v);
        itemCount.addValue(1L);
      }
    }
  }
}
