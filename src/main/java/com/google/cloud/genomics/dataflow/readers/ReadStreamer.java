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

import com.google.api.services.genomics.model.ReadGroupSet;
import com.google.api.services.genomics.model.SearchReadGroupSetsRequest;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.Paginator;
import com.google.cloud.genomics.utils.grpc.Channels;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.genomics.v1.Read;
import com.google.genomics.v1.StreamReadsRequest;
import com.google.genomics.v1.StreamReadsResponse;
import com.google.genomics.v1.StreamingReadServiceGrpc;

/**
 * Class with tools for streaming reads using gRPC within dataflow pipelines.
 */
public class ReadStreamer {

  // TODO should be replaced with a better heuristic
  private static final long SHARD_SIZE = 5000000L; 
  
  /**
   * Gets ReadGroupSetIds from a given datasetId using the Genomics API.
   */
  public static List<String> getReadGroupSetIds(String datasetId, GenomicsFactory.OfflineAuth auth)
      throws IOException, GeneralSecurityException {
    List<String> output = Lists.newArrayList();
    Iterable<ReadGroupSet> rgs = Paginator.ReadGroupSets.create(
        auth.getGenomics(auth.getDefaultFactory()))
        .search(new SearchReadGroupSetsRequest().setDatasetIds(Lists.newArrayList(datasetId)),
            "readGroupSets/id,nextPageToken");
    for (ReadGroupSet r : rgs) {
      output.add(r.getId());
    }
    if (output.isEmpty()) {
      throw new IOException("Dataset " + datasetId + " does not contain any ReadGroupSets");
    }
    return output;
  }

  /**
   * Constructs a StreamReadsRequest for a readGroupSetId, assuming that the user wants to
   * include all references.
   */
  public static StreamReadsRequest getReadRequests(String readGroupSetId) {
    return StreamReadsRequest.newBuilder()
        .setReadGroupSetId(readGroupSetId)
        .build();
  }

  /**
   * Constructs a StreamReadsRequest for a readGroupSetId and a set of user provided references.
   */
  public static List<StreamReadsRequest> getReadRequests(final String readGroupSetId,
      String references) {
    final Iterable<Contig> shards = Contig.getSpecifiedShards(references, SHARD_SIZE);
    return FluentIterable.from(shards)
        .transform(new Function<Contig, StreamReadsRequest>() {
          @Override
          public StreamReadsRequest apply(Contig shard) {
            return shard.getStreamReadsRequest(readGroupSetId);
          }
        }).toList();
  }

  /**
   * PTransform for streaming reads via gRPC.
   */
  public static class StreamReads extends
      PTransform<PCollection<StreamReadsRequest>, PCollection<Read>> {

    @Override
    public PCollection<Read> apply(PCollection<StreamReadsRequest> input) {
      return input.apply(ParDo.of(new RetrieveReads()))
          .apply(ParDo.of(new ConvergeReadsList()));
    }
  }

  private static class RetrieveReads extends DoFn<StreamReadsRequest, List<Read>> {

    protected Aggregator<Integer, Integer> initializedShardCount;
    protected Aggregator<Integer, Integer> finishedShardCount;

    public RetrieveReads() {
      initializedShardCount = createAggregator("Initialized Shard Count", new Sum.SumIntegerFn());
      finishedShardCount = createAggregator("Finished Shard Count", new Sum.SumIntegerFn());
    }

    @Override
    public void processElement(ProcessContext c) throws IOException {
      initializedShardCount.addValue(1);
      StreamingReadServiceGrpc.StreamingReadServiceBlockingStub readStub
          = StreamingReadServiceGrpc.newBlockingStub(Channels.fromDefaultCreds());
      Iterator<StreamReadsResponse> iter = readStub.streamReads(c.element());
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
  private static class ConvergeReadsList extends DoFn<List<Read>, Read> {

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
