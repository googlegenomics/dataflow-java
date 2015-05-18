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
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;

/**
 * Takes a tuple of 2 collections: Contigs and BAM files and transforms them into
 * a collection of reads by reading BAM files in a sharded manner.
 */
public class ReadBAMTransform extends PTransform<PCollectionTuple, PCollection<Read>> {
  GenomicsFactory.OfflineAuth auth;
  
  public static TupleTag<Contig> CONTIGS_TAG = new TupleTag<>();
  public static TupleTag<String> BAMS_TAG = new TupleTag<>();

  /*
  *  getReadsFromBAMFilesSharded reads a BAM using the API key or the client secrets file.
  * This function can only be called on the client when using the client secrets file to produce
  * the credentials (as the file is not copied to workers).
  */
  public static PCollection<Read> getReadsFromBAMFilesSharded(
          Pipeline p,
          Iterable<Contig> contigs, List<String> BAMFiles) throws IOException, GeneralSecurityException {
      final GCSOptions gcsOptions =
              p.getOptions().as(GCSOptions.class);
      return getReadsFromBAMFilesSharded(
              p,
              GenomicsOptions.Methods.getGenomicsAuth(gcsOptions),
              contigs,
              BAMFiles);

  }
  public static PCollection<Read> getReadsFromBAMFilesSharded(
      Pipeline p, 
      GenomicsFactory.OfflineAuth auth,
      Iterable<Contig> contigs, List<String> BAMFiles) {
      ReadBAMTransform readBAMSTransform = new ReadBAMTransform();
      readBAMSTransform.setAuth(auth);
      PCollectionTuple tuple = PCollectionTuple
          .of(
              ReadBAMTransform.BAMS_TAG, 
                p.apply(
                    Create.of(BAMFiles))
                .setCoder(StringUtf8Coder.of()))
         .and(ReadBAMTransform.CONTIGS_TAG, 
             p.apply(
                 Create.of(
                     contigs))
             .setCoder(SerializableCoder.of(Contig.class)));
      return readBAMSTransform.apply(tuple);
  }
  
  public static class ShardFn extends DoFn<String, BAMShard> {
    GenomicsFactory.OfflineAuth auth;
    PCollectionView<Iterable<Contig>> contigsView;
    Storage.Objects storage;
    
    public ShardFn(GenomicsFactory.OfflineAuth auth, PCollectionView<Iterable<Contig>> contigsView) {
      this.auth = auth;
      this.contigsView = contigsView;
    }
    
    @Override
    public void startBundle(DoFn<String, BAMShard>.Context c) throws IOException {
      storage = GCSOptions.Methods.createStorageClient(c, auth);
    }
    
    @Override
    public void processElement(ProcessContext c) throws java.lang.Exception {
      (new Sharder(storage, c.element(), c.sideInput(contigsView), c))
          .process();
    }
  }
  
  public static class ReadFn extends DoFn<BAMShard, Read> {
    GenomicsFactory.OfflineAuth auth;
    Storage.Objects storage;
    
    public ReadFn(GenomicsFactory.OfflineAuth auth) {
      this.auth = auth;

    }
    
    @Override
    public void startBundle(DoFn<BAMShard, Read>.Context c) throws IOException {
      storage = GCSOptions.Methods.createStorageClient(c, auth);
    }
    
    @Override
    public void processElement(ProcessContext c) throws java.lang.Exception {
      (new Reader(storage, c.element(), c))
          .process();
    }
  }
  
  
  @Override
  public PCollection<Read> apply(PCollectionTuple contigsAndBAMs) {

    final PCollection<Contig> contigs = contigsAndBAMs.get(CONTIGS_TAG);
    final PCollectionView<Iterable<Contig>> contigsView =
        contigs.apply(View.<Contig>asIterable());

    final PCollection<String> BAMFileGCSPaths = contigsAndBAMs.get(BAMS_TAG);
   
    final PCollection<BAMShard> shards =
        BAMFileGCSPaths.apply(ParDo
            .withSideInputs(Arrays.asList(contigsView))
            .of(new ShardFn(auth, contigsView)))
              .setCoder(SerializableCoder.of(BAMShard.class));
    
    final PCollection<Read> reads = shards.apply(ParDo
        .of(new ReadFn(auth)));

    return reads;
  }

  public GenomicsFactory.OfflineAuth  getAuth() {
    return auth;
  }

  public void setAuth(GenomicsFactory.OfflineAuth auth) {
    this.auth = auth;
  }
}
