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

import com.google.api.services.genomics.model.SearchVariantSetsRequest;
import com.google.api.services.genomics.model.VariantSet;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.grpc.Channels;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.Paginator;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.StreamVariantsResponse;
import com.google.genomics.v1.StreamingVariantServiceGrpc;
import com.google.genomics.v1.Variant;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Iterator;
import java.util.List;

/**
 * Class with tools for streaming variants using gRPC within dataflow pipelines.
 */
public class VariantStreamer {

  // TODO should be replaced with a better heuristic
  private static final long SHARD_SIZE = 5000000L; 
  
  /**
   * Gets VariantSetIds from a given datasetId using the Genomics API.
   */
  public static List<String> getVariantSetIds(String datasetId, GenomicsFactory.OfflineAuth auth)
      throws IOException, GeneralSecurityException {
    List<String> output = Lists.newArrayList();
    Iterable<VariantSet> vs = Paginator.Variantsets.create(
        auth.getGenomics(auth.getDefaultFactory()))
        .search(new SearchVariantSetsRequest().setDatasetIds(Lists.newArrayList(datasetId)),
            "variantSets/id,nextPageToken");
    for (VariantSet v : vs) {
      output.add(v.getId());
    }
    if (output.isEmpty()) {
      throw new IOException("Dataset " + datasetId + " does not contain any VariantSets");
    }
    return output;
  }

  /**
   * Constructs a StreamVariantsRequest for a variantSetId, assuming that the user wants all
   * to include all references.
   */
  public static StreamVariantsRequest getVariantRequests(String variantSetId) {
    return StreamVariantsRequest.newBuilder()
        .setVariantSetId(variantSetId)
        .build();
  }

  /**
   * Constructs a StreamVariantsRequest for a variantSetId and a set of user provided references.
   */
  public static List<StreamVariantsRequest> getVariantRequests(final String variantSetId,
      String references) {
    final Iterable<Contig> contigs = Contig.parseContigsFromCommandLine(references);
    return FluentIterable.from(contigs)
        .transformAndConcat(new Function<Contig, Iterable<Contig>>() {
            @Override
            public Iterable<Contig> apply(Contig contig) {
              return contig.getShards(SHARD_SIZE);
            }
          })
        .transform(new Function<Contig, StreamVariantsRequest>() {
          @Override
          public StreamVariantsRequest apply(Contig shard) {
            return StreamVariantsRequest.newBuilder()
                .setVariantSetId(variantSetId)
                .setStart(shard.start)
                .setEnd(shard.end)
                .setReferenceName(shard.referenceName)
                .build();
          }
        }).toList();
  }

  /**
   * PTransform for streaming variants via gRPC.
   */
  public static class StreamVariants extends
      PTransform<PCollection<StreamVariantsRequest>, PCollection<Variant>> {

    @Override
    public PCollection<Variant> apply(PCollection<StreamVariantsRequest> input) {
      return input.apply(ParDo.of(new RetrieveVariants()))
          .apply(ParDo.of(new ConvergeVariantsList()));
    }
  }

  private static class RetrieveVariants extends DoFn<StreamVariantsRequest, List<Variant>> {

    protected Aggregator<Integer> initializedShardCount;
    protected Aggregator<Integer> finishedShardCount;

    @Override
    public void startBundle(Context c) throws IOException {
      initializedShardCount = c.createAggregator("Initialized Shard Count", new Sum.SumIntegerFn());
      finishedShardCount = c.createAggregator("Finished Shard Count", new Sum.SumIntegerFn());
    }

    @Override
    public void processElement(ProcessContext c) throws IOException {
      initializedShardCount.addValue(1);
      StreamingVariantServiceGrpc.StreamingVariantServiceBlockingStub variantStub =
          StreamingVariantServiceGrpc.newBlockingStub(Channels.fromDefaultCreds());
      Iterator<StreamVariantsResponse> iter = variantStub.streamVariants(c.element());
      while (iter.hasNext()) {
        StreamVariantsResponse variantResponse = iter.next();
        c.output(variantResponse.getVariantsList());
      }
      finishedShardCount.addValue(1);
    }
  }

  /**
   * This step exists to emit the individual variants in a parallel step to the StreamVariants step
   * in order to increase throughput.
   */
  private static class ConvergeVariantsList extends DoFn<List<Variant>, Variant> {

    protected Aggregator<Long> itemCount;

    @Override
    public void startBundle(Context c) {
      itemCount = c.createAggregator("Number of variants", new Sum.SumLongFn());
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
