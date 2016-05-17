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
package com.google.cloud.genomics.dataflow.pipelines;

import com.google.api.services.storage.Storage;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DelegateCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.functions.ShardReadsTransform;
import com.google.cloud.genomics.dataflow.readers.ReadStreamer;
import com.google.cloud.genomics.dataflow.readers.bam.HeaderInfo;
import com.google.cloud.genomics.dataflow.readers.bam.ReadBAMTransform;
import com.google.cloud.genomics.dataflow.readers.bam.ReaderOptions;
import com.google.cloud.genomics.dataflow.readers.bam.ShardingPolicy;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GCSOutputOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.dataflow.writers.bam.WriteBAMTransform;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.cloud.genomics.utils.ShardUtils.SexChromosomeFilter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.genomics.v1.Read;
import com.google.genomics.v1.StreamReadsRequest;

import htsjdk.samtools.ValidationStringency;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * Demonstrates loading some Reads, sharding them, writing them to BAM file shards in parallel,
 * then combining the shards and writing an index for the combined BAM file.
 */
public class ShardedBAMWriting {

  static interface Options extends ShardOptions, ShardReadsTransform.Options,
    WriteBAMTransform.Options, GCSOutputOptions {
    @Description("The Google Cloud Storage path to the BAM file to get reads data from" +
        "This or ReadGroupSetId must be set")
    @Default.String("")
    String getBAMFilePath();

    void setBAMFilePath(String filePath);

    @Description("An ID of the Google Genomics ReadGroupSets this " +
        "pipeline is working with. This or BAMFilePath must be set.")
    @Default.String("")
    String getReadGroupSetId();

    void setReadGroupSetId(String readGroupSetId);

    public static class Methods {
      public static void validateOptions(Options options) {
        GCSOutputOptions.Methods.validateOptions(options);
        Preconditions.checkArgument(
            !Strings.isNullOrEmpty(options.getReadGroupSetId()) ||
            !Strings.isNullOrEmpty(options.getBAMFilePath()),
            "Either BAMFilePath or ReadGroupSetId must be specified");
      }
    }
  }

  private static final Logger LOG = Logger.getLogger(ShardedBAMWriting.class.getName());
  private static Options pipelineOptions;
  private static Pipeline pipeline;
  private static OfflineAuth auth;
  private static Iterable<Contig> contigs;

  public static void main(String[] args) throws GeneralSecurityException, IOException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(Options.class);
    pipelineOptions = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    Options.Methods.validateOptions(pipelineOptions);

    auth = GenomicsOptions.Methods.getGenomicsAuth(pipelineOptions);
    pipeline = Pipeline.create(pipelineOptions);
    pipeline.getCoderRegistry().registerCoder(Contig.class, CONTIG_CODER);
    // Process options.
    contigs = pipelineOptions.isAllReferences() ? null :
      Contig.parseContigsFromCommandLine(pipelineOptions.getReferences());


    // Get the reads and shard them.
    PCollection<Read> reads;
    HeaderInfo headerInfo;

    final String outputFileName = pipelineOptions.getOutput();
    final GcsPath destPath = GcsPath.fromUri(outputFileName);
    final GcsPath destIdxPath = GcsPath.fromUri(outputFileName + ".bai");
    final Storage.Objects storage = Transport.newStorageClient(
        pipelineOptions
          .as(GCSOptions.class))
          .build()
          .objects();
    LOG.info("Cleaning up output file " + destPath + " and " + destIdxPath);
    try {
      storage.delete(destPath.getBucket(), destPath.getObject()).execute();
    } catch (Exception ignored) {
      // Ignore errors
    }
    try {
      storage.delete(destIdxPath.getBucket(), destIdxPath.getObject()).execute();
    } catch (Exception ignored) {
      // Ignore errors
    }

    if (!Strings.isNullOrEmpty(pipelineOptions.getReadGroupSetId())) {
      headerInfo = HeaderInfo.getHeaderFromApi(pipelineOptions.getReadGroupSetId(), auth, contigs);
      reads = getReadsFromAPI();
    } else {
      headerInfo = HeaderInfo.getHeaderFromBAMFile(storage, pipelineOptions.getBAMFilePath(), contigs);
      reads = getReadsFromBAMFile();
    }

    final PCollection<String> writtenFiles = WriteBAMTransform.write(
        reads, headerInfo, pipelineOptions.getOutput(), pipeline);

    writtenFiles
        .apply(
            TextIO.Write
              .to(pipelineOptions.getOutput() + "-result")
        .named("Write Output Result")
        .withoutSharding());
    pipeline.run();
  }

  private static PCollection<Read> getReadsFromBAMFile() throws IOException {
    /**
     * Policy used to shard Reads.
     * By default we are using the default sharding supplied by the policy class.
     * If you want custom sharding, use the following pattern:
     * <pre>
     *    BAM_FILE_READ_SHARDING_POLICY = new ShardingPolicy() {
     *     @Override
     *     public boolean shardBigEnough(BAMShard shard) {
     *       return shard.sizeInLoci() > 50000000;
     *     }
     *   };
     * </pre>
     */
    final ShardingPolicy BAM_FILE_READ_SHARDING_POLICY = ShardingPolicy.BYTE_SIZE_POLICY;

    LOG.info("Sharded reading of " + pipelineOptions.getBAMFilePath());

    final ReaderOptions readerOptions = new ReaderOptions(
        ValidationStringency.DEFAULT_STRINGENCY,
        true);

    return ReadBAMTransform.getReadsFromBAMFilesSharded(pipeline,
        auth,
        contigs,
        readerOptions,
        pipelineOptions.getBAMFilePath(),
        BAM_FILE_READ_SHARDING_POLICY);
  }

  private static PCollection<Read> getReadsFromAPI() throws IOException {
    final String  rgsId = pipelineOptions.getReadGroupSetId();
    LOG.info("Sharded reading of ReadGroupSet: " + rgsId);

    List<StreamReadsRequest> requests = Lists.newArrayList();

    if (pipelineOptions.isAllReferences()) {
      requests.addAll(ShardUtils.getReadRequests(rgsId, SexChromosomeFilter.INCLUDE_XY,
          pipelineOptions.getBasesPerShard(), auth));
    } else {
      requests.addAll(
          ShardUtils.getReadRequests(Collections.singletonList(rgsId),
              pipelineOptions.getReferences(), pipelineOptions.getBasesPerShard()));
    }

    LOG.info("Reading from the API with: " + requests.size() + " shards");

    PCollection<Read> reads = pipeline.apply(Create.of(requests))
        .apply(new ReadStreamer(auth, ShardBoundary.Requirement.STRICT, null));
    return reads;
  }

  static Coder<Contig> CONTIG_CODER = DelegateCoder.of(
      StringUtf8Coder.of(),
      new DelegateCoder.CodingFunction<Contig,String>() {
        @Override
        public String apply(Contig contig) throws Exception {
          return contig.toString();
        }
      },
      new DelegateCoder.CodingFunction<String, Contig>() {
        @Override
        public Contig apply(String contigStr) throws Exception {
          return Contig.parseContigsFromCommandLine(contigStr).iterator().next();
        }
      });
}
