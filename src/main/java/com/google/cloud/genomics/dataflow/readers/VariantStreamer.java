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

import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.grpc.VariantStreamIterator;
import com.google.common.base.Stopwatch;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.StreamVariantsResponse;
import com.google.genomics.v1.Variant;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * PTransform for streaming variants via gRPC.
 */
public class VariantStreamer extends
PTransform<PCollection<StreamVariantsRequest>, PCollection<Variant>> {

  private static final Logger LOG = LoggerFactory.getLogger(VariantStreamer.class);
  protected final OfflineAuth auth;
  protected final ShardBoundary.Requirement shardBoundary;
  protected final String fields;

  /**
   * Create a streamer that can enforce shard boundary semantics.
   *
   * Tip: Use the API explorer to test which fields to include in partial responses:
   * <a href="https://developers.google.com/apis-explorer/#p/genomics/v1/genomics.variants.stream?fields=variants(alternateBases%252Ccalls(callSetName%252Cgenotype)%252CreferenceBases)&_h=3&resource=%257B%250A++%2522variantSetId%2522%253A+%25223049512673186936334%2522%252C%250A++%2522referenceName%2522%253A+%2522chr17%2522%252C%250A++%2522start%2522%253A+%252241196311%2522%252C%250A++%2522end%2522%253A+%252241196312%2522%252C%250A++%2522callSetIds%2522%253A+%250A++%255B%25223049512673186936334-0%2522%250A++%255D%250A%257D&">
   * variants example</a>.
   *
   * @param auth The OfflineAuth to use for the request.
   * @param shardBoundary The shard boundary semantics to enforce.
   * @param fields Which fields to include in a partial response or null for all.
   */
  public VariantStreamer(OfflineAuth auth, ShardBoundary.Requirement shardBoundary, String fields) {
    this.auth = auth;
    this.shardBoundary = shardBoundary;
    this.fields = fields;
  }

  @Override
  public PCollection<Variant> expand(PCollection<StreamVariantsRequest> input) {
    return input.apply(ParDo.of(new RetrieveVariants()))
        .apply(ParDo.of(new ConvergeVariantsList()));
  }

  private class RetrieveVariants extends DoFn<StreamVariantsRequest, List<Variant>> {
    DescriptiveStatistics stats;

    public RetrieveVariants() {
      stats = new DescriptiveStatistics(500);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException, GeneralSecurityException, InterruptedException {
      Metrics.counter(RetrieveVariants.class, "Initialized Shard Count").inc();
      Stopwatch stopWatch = Stopwatch.createStarted();
      Iterator<StreamVariantsResponse> iter = VariantStreamIterator.enforceShardBoundary(auth, c.element(), shardBoundary, fields);
      while (iter.hasNext()) {
        StreamVariantsResponse variantResponse = iter.next();
        c.output(variantResponse.getVariantsList());
      }
      stopWatch.stop();
      Metrics.distribution(RetrieveVariants.class, "Shard Processing Time (sec)")
          .update(stopWatch.elapsed(TimeUnit.SECONDS));
      Metrics.counter(RetrieveVariants.class, "Finished Shard Count").inc();
      stats.addValue(stopWatch.elapsed(TimeUnit.SECONDS));
      LOG.info("Shard Duration in Seconds - Min: " + stats.getMin() + " Max: " + stats.getMax() +
          " Avg: " + stats.getMean() + " StdDev: " + stats.getStandardDeviation());
    }
  }

  /**
   * This step exists to emit the individual variants in a parallel step to the StreamVariants step
   * in order to increase throughput.
   */
  private class ConvergeVariantsList extends DoFn<List<Variant>, Variant> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      for (Variant v : c.element()) {
        c.output(v);
        Metrics.counter(ConvergeVariantsList.class, "Number of variants").inc();
      }
    }
  }
}
