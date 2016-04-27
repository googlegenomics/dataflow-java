/*
 * Copyright (C) 2015 Google Inc.
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
package com.google.cloud.genomics.dataflow.readers.bam;

import com.google.api.services.storage.Storage;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum.SumIntegerFn;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.genomics.v1.Read;

import java.io.IOException;
import java.util.List;

/**
 * Takes a tuple of 2 collections: Contigs and BAM files and transforms them into
 * a collection of reads by reading BAM files in a sharded manner.
 */
public class ReadBAMTransform extends PTransform<PCollection<BAMShard>, PCollection<Read>> {
  OfflineAuth auth;
  ReaderOptions options;

  public static class ReadFn extends DoFn<BAMShard, Read> {
    OfflineAuth auth;
    Storage.Objects storage;
    ReaderOptions options;
    Aggregator<Integer, Integer> recordCountAggregator;
    Aggregator<Integer, Integer> readCountAggregator;
    Aggregator<Integer, Integer> skippedStartCountAggregator;
    Aggregator<Integer, Integer> skippedEndCountAggregator;
    Aggregator<Integer, Integer> skippedRefMismatchAggregator;

    public ReadFn(OfflineAuth auth, ReaderOptions options) {
      this.auth = auth;
      this.options = options;
      recordCountAggregator = createAggregator("Processed records", new SumIntegerFn());
      readCountAggregator = createAggregator("Reads generated", new SumIntegerFn());
      skippedStartCountAggregator = createAggregator("Skipped start", new SumIntegerFn());
      skippedEndCountAggregator = createAggregator("Skipped end", new SumIntegerFn());
      skippedRefMismatchAggregator = createAggregator("Ref mismatch", new SumIntegerFn());
    }

    @Override
    public void startBundle(DoFn<BAMShard, Read>.Context c) throws IOException {
      storage = Transport.newStorageClient(c.getPipelineOptions().as(GCSOptions.class)).build().objects();
    }

    @Override
    public void processElement(ProcessContext c) throws java.lang.Exception {
      final Reader reader = new Reader(storage, options, c.element(), c);
      reader.process();
      recordCountAggregator.addValue(reader.recordsProcessed);
      skippedStartCountAggregator.addValue(reader.recordsBeforeStart);
      skippedEndCountAggregator.addValue(reader.recordsAfterEnd);
      skippedRefMismatchAggregator.addValue(reader.mismatchedSequence);
      readCountAggregator.addValue(reader.readsGenerated);
    }
  }

  // ----------------------------------------------------------------
  // back to ReadBAMTransform

  public static PCollection<Read> getReadsFromBAMFilesSharded(
      Pipeline p,
      OfflineAuth auth,
      Iterable<Contig> contigs,
      ReaderOptions options,
      String BAMFile,
      ShardingPolicy shardingPolicy) throws IOException {
      ReadBAMTransform readBAMSTransform = new ReadBAMTransform(options);
      readBAMSTransform.setAuth(auth);

      final Storage.Objects storage = Transport
          .newStorageClient(p.getOptions().as(GCSOptions.class)).build().objects();


      final List<BAMShard> shardsList = Sharder.shardBAMFile(storage, BAMFile, contigs,
         shardingPolicy);

      PCollection<BAMShard> shards = p.apply(Create
          .of(shardsList))
          .setCoder(SerializableCoder.of(BAMShard.class));

      return readBAMSTransform.apply(shards);
  }

  @Override
  public PCollection<Read> apply(PCollection<BAMShard> shards) {
    final PCollection<Read> reads = shards.apply(ParDo
        .of(new ReadFn(auth, options)));

    return reads;
  }

  public OfflineAuth  getAuth() {
    return auth;
  }

  public void setAuth(OfflineAuth auth) {
    this.auth = auth;
  }

  // non-public methods

  protected ReadBAMTransform(ReaderOptions options) {
    super();
    this.options = options;
  }
}
