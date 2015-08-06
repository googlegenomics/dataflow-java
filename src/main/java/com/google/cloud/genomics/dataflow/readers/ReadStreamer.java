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

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.grpc.Channels;
import com.google.cloud.genomics.utils.grpc.ReadStreamIterator;
import com.google.genomics.v1.Read;
import com.google.genomics.v1.StreamReadsRequest;
import com.google.genomics.v1.StreamReadsResponse;
import com.google.genomics.v1.StreamingReadServiceGrpc;

/**
 * PTransform for streaming reads via gRPC.
 */
public class ReadStreamer extends
PTransform<PCollection<StreamReadsRequest>, PCollection<Read>> {

  protected final GenomicsFactory.OfflineAuth auth;

  public ReadStreamer(GenomicsFactory.OfflineAuth auth) {
    this.auth = auth;
  }

  @Override
  public PCollection<Read> apply(PCollection<StreamReadsRequest> input) {
    return input.apply(ParDo.of(new RetrieveReads()))
        .apply(ParDo.of(new ConvergeReadsList()));
  }


  private class RetrieveReads extends DoFn<StreamReadsRequest, List<Read>> {

    protected Aggregator<Integer, Integer> initializedShardCount;
    protected Aggregator<Integer, Integer> finishedShardCount;

    public RetrieveReads() {
      initializedShardCount = createAggregator("Initialized Shard Count", new Sum.SumIntegerFn());
      finishedShardCount = createAggregator("Finished Shard Count", new Sum.SumIntegerFn());
    }

    @Override
    public void processElement(ProcessContext c) throws IOException, GeneralSecurityException {
      initializedShardCount.addValue(1);
      Iterator<StreamReadsResponse> iter = new ReadStreamIterator(c.element(), auth);
      while (iter.hasNext()) {
        StreamReadsResponse readResponse = iter.next();
        c.output(readResponse.getAlignmentsList());
      }
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

