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

import com.google.api.services.genomics.model.Read;
import com.google.api.services.storage.Storage;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;

import htsjdk.samtools.ValidationStringency;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Takes a tuple of 2 collections: Contigs and BAM files and transforms them into
 * a collection of reads by reading BAM files in a sharded manner.
 */
public class ReadBAMTransform extends PTransform<PCollection<BAMShard>, PCollection<Read>> {
  GenomicsFactory.OfflineAuth auth;
  ReaderOptions options;

  public static class ReadFn extends DoFn<BAMShard, Read> {
    GenomicsFactory.OfflineAuth auth;
    Storage.Objects storage;
    ReaderOptions options;

    public ReadFn(GenomicsFactory.OfflineAuth auth, ReaderOptions options) {
      this.auth = auth;
      this.options = options;
    }

    @Override
    public void startBundle(DoFn<BAMShard, Read>.Context c) throws IOException {
      storage = Transport.newStorageClient(c.getPipelineOptions().as(GCSOptions.class)).build().objects();
    }

    @Override
    public void processElement(ProcessContext c) throws java.lang.Exception {
      (new Reader(storage, options, c.element(), c))
          .process();
    }
  }

  // ----------------------------------------------------------------
  // back to ReadBAMTransform

  public static PCollection<Read> getReadsFromBAMFilesSharded(
      Pipeline p, 
      GenomicsFactory.OfflineAuth auth,
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

  public GenomicsFactory.OfflineAuth  getAuth() {
    return auth;
  }

  public void setAuth(GenomicsFactory.OfflineAuth auth) {
    this.auth = auth;
  }

  // non-public methods

  protected ReadBAMTransform(ReaderOptions options) {
    super();
    this.options = options;
  }
}
