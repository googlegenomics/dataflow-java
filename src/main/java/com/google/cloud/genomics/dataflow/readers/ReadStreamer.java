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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.grpc.ReadStreamIterator;
import com.google.common.base.Stopwatch;
import com.google.genomics.v1.Read;
import com.google.genomics.v1.StreamReadsRequest;
import com.google.genomics.v1.StreamReadsResponse;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * PTransform for streaming reads via gRPC.
 */
public class ReadStreamer extends
PTransform<PCollection<StreamReadsRequest>, PCollection<Read>> {

  protected final OfflineAuth auth;
  protected final ShardBoundary.Requirement shardBoundary;
  protected final String fields;

  /**
   * Create a streamer that can enforce shard boundary semantics.
   *
   * Tip: Use the API explorer to test which fields to include in partial responses:
   * <a href="https://developers.google.com/apis-explorer/#p/genomics/v1/genomics.reads.stream?fields=alignments(alignedSequence%252Cid)&_h=2&resource=%257B%250A++%2522readGroupSetId%2522%253A+%2522CMvnhpKTFhD3he72j4KZuyc%2522%252C%250A++%2522referenceName%2522%253A+%2522chr17%2522%252C%250A++%2522start%2522%253A+%252241196311%2522%252C%250A++%2522end%2522%253A+%252241196312%2522%250A%257D&">
   * reads example</a>.
   *
   * @param auth The OfflineAuth to use for the request.
   * @param shardBoundary The shard boundary semantics to enforce.
   * @param fields Which fields to include in a partial response or null for all.
   */
  public ReadStreamer(OfflineAuth auth, ShardBoundary.Requirement shardBoundary, String fields) {
    this.auth = auth;
    this.shardBoundary = shardBoundary;
    this.fields = fields;
  }

  @Override
  public PCollection<Read> expand(PCollection<StreamReadsRequest> input) {
    return input.apply(ParDo.of(new RetrieveReads()))
        .apply(ParDo.of(new ConvergeReadsList()));
  }


  private class RetrieveReads extends DoFn<StreamReadsRequest, List<Read>> {

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException, GeneralSecurityException {
      Metrics.counter(RetrieveReads.class, "Initialized Shard Count").inc();
      Stopwatch stopWatch = Stopwatch.createStarted();
      Iterator<StreamReadsResponse> iter = ReadStreamIterator.enforceShardBoundary(auth, c.element(), shardBoundary, fields);
      while (iter.hasNext()) {
        StreamReadsResponse readResponse = iter.next();
        c.output(readResponse.getAlignmentsList());
      }
      stopWatch.stop();
      Metrics.distribution(RetrieveReads.class, "Shard Processing Time (sec)")
          .update(stopWatch.elapsed(TimeUnit.SECONDS));
      Metrics.counter(RetrieveReads.class, "Finished Shard Count").inc();
    }
  }

  /**
   * This step exists to emit the individual reads in a parallel step to the StreamReads step in
   * order to increase throughput.
   */
  private class ConvergeReadsList extends DoFn<List<Read>, Read> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      for (Read r : c.element()) {
        c.output(r);
        Metrics.counter(ConvergeReadsList.class, "Number of reads").inc();
      }
    }
  }
}

