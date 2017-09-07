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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.sdk.values.PCollection;
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

    public ReadFn(OfflineAuth auth, ReaderOptions options) {
      this.auth = auth;
      this.options = options;
    }

    @StartBundle
    public void startBundle(DoFn<BAMShard, Read>.StartBundleContext c) throws IOException {
      storage = Transport.newStorageClient(c.getPipelineOptions().as(GCSOptions.class)).build().objects();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws java.lang.Exception {
      final Reader reader = new Reader(storage, options, c.element(), c);
      reader.process();
      Metrics.counter(ReadBAMTransform.class, "Processed records").inc(reader.recordsProcessed);
      Metrics.counter(ReadBAMTransform.class, "Reads generated").inc(reader.readsGenerated);
      Metrics.counter(ReadBAMTransform.class, "Skipped start").inc(reader.recordsBeforeStart);
      Metrics.counter(ReadBAMTransform.class, "Skipped end").inc(reader.recordsAfterEnd);
      Metrics.counter(ReadBAMTransform.class, "Ref mismatch").inc(reader.mismatchedSequence);

    }
  }

  // ----------------------------------------------------------------
  // back to ReadBAMTransform

  public static PCollection<Read> getReadsFromBAMFilesSharded(
      Pipeline p,
      PipelineOptions pipelineOptions,
      OfflineAuth auth,
      Iterable<Contig> contigs,
      ReaderOptions options,
      String BAMFile,
      ShardingPolicy shardingPolicy) throws IOException {
      ReadBAMTransform readBAMSTransform = new ReadBAMTransform(options);
      readBAMSTransform.setAuth(auth);

      final Storage.Objects storage = Transport
          .newStorageClient(pipelineOptions.as(GCSOptions.class)).build().objects();


      final List<BAMShard> shardsList = Sharder.shardBAMFile(storage, BAMFile, contigs,
         shardingPolicy);

      PCollection<BAMShard> shards = p.apply(Create
          .of(shardsList))
          .setCoder(SerializableCoder.of(BAMShard.class));

      return readBAMSTransform.expand(shards);
  }

  @Override
  public PCollection<Read> expand(PCollection<BAMShard> shards) {
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
