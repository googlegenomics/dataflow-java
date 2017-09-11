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
import com.google.cloud.genomics.dataflow.functions.BreakFusionTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.genomics.v1.Read;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Takes a tuple of 2 collections: Contigs and BAM files and transforms them into
 * a collection of reads by reading BAM files in a sharded manner.
 */
public class ReadBAMTransform extends PTransform<PCollection<BAMShard>, PCollection<Read>> {
  private static final Logger LOG = Logger.getLogger(ReadBAMTransform.class.getName());
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


  /**
   * Get reads from a single BAM file by serially reading one shard at a time.
   *
   * This is useful when reads from a subset of genomic regions is desired.
   *
   * This method is marked as deprecated because getReadsFromBAMFilesSharded offers
   * the same functionality but shard reading occurs in parallel.
   *
   * This method should be removed when https://github.com/googlegenomics/dataflow-java/issues/214
   * is fixed.
   *
   * @param p
   * @param pipelineOptions
   * @param auth
   * @param contigs
   * @param options
   * @param BAMFile
   * @param shardingPolicy
   * @return
   * @throws IOException
   */
  @Deprecated
  public static PCollection<Read> getReadsFromBAMFileSharded(
      Pipeline p,
      PipelineOptions pipelineOptions,
      OfflineAuth auth,
      List<Contig> contigs,
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
        .of(shardsList));
    return readBAMSTransform.expand(shards);
  }

  /**
   * Get reads from one or more BAM files by reading shards in parallel.
   *
   * @param p
   * @param pipelineOptions
   * @param auth
   * @param contigs
   * @param options
   * @param bamFileOrGlob
   * @param shardingPolicy
   * @return
   * @throws IOException
   * @throws URISyntaxException
   */
  public static PCollection<Read> getReadsFromBAMFilesSharded(
      Pipeline p,
      PipelineOptions pipelineOptions,
      OfflineAuth auth,
      final List<Contig> contigs,
      ReaderOptions options,
      String bamFileOrGlob,
      final ShardingPolicy shardingPolicy) throws IOException, URISyntaxException {
      ReadBAMTransform readBAMSTransform = new ReadBAMTransform(options);
      readBAMSTransform.setAuth(auth);

      Set<String> uris = new HashSet<>();
      GcsUtil gcsUtil = pipelineOptions.as(GcsOptions.class).getGcsUtil();
      URI absoluteUri = new URI(bamFileOrGlob);
      URI gcsUriGlob = new URI(
          absoluteUri.getScheme(),
          absoluteUri.getAuthority(),
          absoluteUri.getPath() + "*",
          absoluteUri.getQuery(),
          absoluteUri.getFragment());
      for (GcsPath entry : gcsUtil.expand(GcsPath.fromUri(gcsUriGlob))) {
        // Even if provided with an exact match to a particular BAM file, the glob operation will
        // still look for any files with that prefix, therefore also finding the corresponding
        // .bai file. Ensure only BAMs are added to the list.
        if (entry.toString().endsWith(BAMIO.BAM_FILE_SUFFIX)) {
          uris.add(entry.toString());
        }
      }

      // Perform all sharding and reading in a distributed fashion, using the BreakFusion
      // transforms to ensure that work is auto-scalable based on the number of shards.
      return p
        .apply(Create.of(uris))
        .apply("Break BAM file fusion", new BreakFusionTransform<String>())
        .apply("BamsToShards", ParDo.of(new DoFn<String, BAMShard>() {
          Storage.Objects storage;
          @StartBundle
          public void startBundle(DoFn<String, BAMShard>.StartBundleContext c) throws IOException {
            storage = Transport.newStorageClient(c.getPipelineOptions().as(GCSOptions.class)).build().objects();
          }
          @DoFn.ProcessElement
          public void processElement(DoFn<String, BAMShard>.ProcessContext c) {
            List<BAMShard> shardsList = null;
            try {
              shardsList = Sharder.shardBAMFile(storage, c.element(), contigs, shardingPolicy);
              LOG.info("Sharding BAM " + c.element());
              Metrics.counter(ReadBAMTransform.class, "BAM files").inc();
              Metrics.counter(ReadBAMTransform.class, "BAM file shards").inc(shardsList.size());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            for (BAMShard shard : shardsList) {
              c.output(shard);
            }
          }
        }))
        // We need a BreakFusionTransform here but BAMShard does not have a deterministic coder, a
        // requirement for values that are keys.  Send it as a value instead.
        .apply("Break BAMShard fusion - group", ParDo.of(new DoFn<BAMShard, KV<String, BAMShard>>() {
          @DoFn.ProcessElement
          public void processElement(DoFn<BAMShard, KV<String, BAMShard>>.ProcessContext c) throws Exception {
            c.output(KV.of(c.element().toString(), c.element()));
          }
        }))
        .apply("Break BAMShard fusion - shuffle", GroupByKey.<String, BAMShard>create())
        .apply("Break BAMShard fusion - ungroup", ParDo.of(new DoFn<KV<String, Iterable<BAMShard>>, BAMShard>() {
          @DoFn.ProcessElement
          public void processElement(DoFn<KV<String, Iterable<BAMShard>>, BAMShard>.ProcessContext c) {
            for (BAMShard shard : c.element().getValue()) {
              c.output(shard);
            }
          }
        }))
        .apply(readBAMSTransform);
  }

  @Override
  public PCollection<Read> expand(PCollection<BAMShard> shards) {
    final PCollection<Read> reads = shards.apply("Read reads from BAMShards", ParDo
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
