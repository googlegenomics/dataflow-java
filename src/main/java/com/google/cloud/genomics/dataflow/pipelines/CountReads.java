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
import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.readers.ReadGroupStreamer;
import com.google.cloud.genomics.dataflow.readers.bam.BAMShard;
import com.google.cloud.genomics.dataflow.readers.bam.ReadBAMTransform;
import com.google.cloud.genomics.dataflow.readers.bam.Reader;
import com.google.cloud.genomics.dataflow.readers.bam.ReaderOptions;
import com.google.cloud.genomics.dataflow.readers.bam.ShardingPolicy;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GCSOutputOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils.SexChromosomeFilter;
import com.google.common.base.Strings;
import com.google.genomics.v1.Read;

import htsjdk.samtools.ValidationStringency;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.logging.Logger;

/**
 * Simple read counting pipeline, intended as an example for reading data from
 * APIs OR BAM files and invoking GATK tools.
 *
 * See http://googlegenomics.readthedocs.org/en/latest/use_cases/analyze_reads/count_reads.html
 * for running instructions.
 */
public class CountReads {

  public static interface Options extends GCSOptions, ShardOptions, GCSOutputOptions {

    @Description("The ID of the Google Genomics ReadGroupSet this pipeline is working with. "
        + "Default (empty) indicates all ReadGroupSets.")
    @Default.String("")
    String getReadGroupSetId();
    void setReadGroupSetId(String readGroupSetId);

    @Description("The Google Cloud Storage path to the BAM file to get reads data from, if not using ReadGroupSet.")
    @Default.String("")
    String getBAMFilePath();
    void setBAMFilePath(String filePath);

    @Description("Whether to shard BAM file reading.")
    @Default.Boolean(true)
    boolean isShardBAMReading();
    void setShardBAMReading(boolean newValue);

    @Description("For shareded BAM file input, the maximum size in bytes of a shard processed by "
        + "a single worker. (For --readGroupSetId shard size is controlled by --basesPerShard.)")
    @Default.Integer(100 * 1024 * 1024)  // 100 MB
    int getMaxShardSizeBytes();
    void setMaxShardSizeBytes(int maxShardSizeBytes);

    @Description("Whether to include unmapped mate pairs of mapped reads to match expectations of Picard tools.")
    @Default.Boolean(false)
    boolean isIncludeUnmapped();
    void setIncludeUnmapped(boolean newValue);

    @Description("Whether to wait until the pipeline completes. This is useful "
      + "for test purposes.")
    @Default.Boolean(false)
    boolean getWait();
    void setWait(boolean wait);

    public static class Methods {
      public static void validateOptions(Options options) {
        GCSOutputOptions.Methods.validateOptions(options);
      }
    }

  }

  // Tip: Use the API explorer to test which fields to include in partial responses.
  // https://developers.google.com/apis-explorer/#p/genomics/v1/genomics.reads.stream?fields=alignments(alignedSequence%252Cid)&_h=2&resource=%257B%250A++%2522readGroupSetId%2522%253A+%2522CMvnhpKTFhD3he72j4KZuyc%2522%252C%250A++%2522referenceName%2522%253A+%2522chr17%2522%252C%250A++%2522start%2522%253A+%252241196311%2522%252C%250A++%2522end%2522%253A+%252241196312%2522%250A%257D&
  private static final String READ_FIELDS = "alignments(alignment,id)";
  private static final Logger LOG = Logger.getLogger(CountReads.class.getName());
  private static Pipeline p;
  private static Options pipelineOptions;
  private static OfflineAuth auth;

  public static void main(String[] args) throws GeneralSecurityException, IOException, URISyntaxException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(Options.class);
    pipelineOptions = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    Options.Methods.validateOptions(pipelineOptions);

    auth = GenomicsOptions.Methods.getGenomicsAuth(pipelineOptions);
    p = Pipeline.create(pipelineOptions);

    // ensure data is accessible
    String BAMFilePath = pipelineOptions.getBAMFilePath();
    if (!Strings.isNullOrEmpty(BAMFilePath)) {
      if (GCSURLExists(BAMFilePath)) {
        System.out.println(BAMFilePath + " is present, good.");
      } else {
        System.out.println("Error: " + BAMFilePath + " not found.");
        return;
      }
      if (pipelineOptions.isShardBAMReading()) {
        // the BAM code expects an index at BAMFilePath+".bai"
        // and sharded reading will fail if the index isn't there.
        String BAMIndexPath = BAMFilePath + ".bai";
        if (GCSURLExists(BAMIndexPath)) {
          System.out.println(BAMIndexPath + " is present, good.");
        } else {
          System.out.println("Error: " + BAMIndexPath + " not found.");
          return;
        }
      }
    }
    System.out.println("Output will be written to "+pipelineOptions.getOutput());

    PCollection<Read> reads = getReads();
    PCollection<Long> readCount = reads.apply(Count.<Read>globally());
    PCollection<String> readCountText = readCount.apply("toString", ParDo.of(new DoFn<Long, String>() {
      @ProcessElement
      public void processElement(DoFn<Long, String>.ProcessContext c) throws Exception {
        c.output(String.valueOf(c.element()));
      }
    }));
    readCountText.apply("WriteOutput", TextIO.write().to(pipelineOptions.getOutput()).withoutSharding());

    PipelineResult result = p.run();
    if(pipelineOptions.getWait()) {
      result.waitUntilFinish();
    }
  }

  private static boolean GCSURLExists(String url) {
    // ensure data is accessible
    try {
      // if we can read the size, then surely we can read the file
      GcsPath fn = GcsPath.fromUri(url);
      Storage.Objects storageClient = GCSOptions.Methods.createStorageClient(pipelineOptions, auth);
      Storage.Objects.Get getter = storageClient.get(fn.getBucket(), fn.getObject());
      StorageObject object = getter.execute();
      BigInteger size = object.getSize();
      return true;
    } catch (Exception x) {
      return false;
    }
  }

  private static PCollection<Read> getReads() throws IOException, URISyntaxException {
    if (!pipelineOptions.getBAMFilePath().isEmpty()) {
      return getReadsFromBAMFile();
    }
    if (!pipelineOptions.getReadGroupSetId().isEmpty()) {
      return getReadsFromAPI();
    }
    throw new IOException("Either BAM file or ReadGroupSet must be specified");
  }

  private static PCollection<Read> getReadsFromAPI() {
    final PCollection<Read> reads = p.begin()
        .apply(Create.of(Collections.singletonList(pipelineOptions.getReadGroupSetId())))
        .apply(new ReadGroupStreamer(auth, ShardBoundary.Requirement.STRICT, READ_FIELDS, SexChromosomeFilter.INCLUDE_XY));
    return reads;
  }

  private static PCollection<Read> getReadsFromBAMFile() throws IOException, URISyntaxException {
    LOG.info("getReadsFromBAMFile");

    final Iterable<Contig> contigs = Contig.parseContigsFromCommandLine(pipelineOptions.getReferences());
    final ReaderOptions readerOptions = new ReaderOptions(
        ValidationStringency.LENIENT,
        pipelineOptions.isIncludeUnmapped());
    if (pipelineOptions.isShardBAMReading()) {
      LOG.info("Sharded reading of "+ pipelineOptions.getBAMFilePath());

      ShardingPolicy policy = new ShardingPolicy() {
        final int MAX_BYTES_PER_SHARD = pipelineOptions.getMaxShardSizeBytes();
        @Override
        public Boolean apply(BAMShard shard) {
          return shard.approximateSizeInBytes() > MAX_BYTES_PER_SHARD;
        }
      };

      return ReadBAMTransform.getReadsFromBAMFilesSharded(p,
          pipelineOptions,
          auth,
          Lists.newArrayList(contigs),
          readerOptions,
          pipelineOptions.getBAMFilePath(),
          policy);
    } else {  // For testing and comparing sharded vs. not sharded only
      LOG.info("Unsharded reading of " + pipelineOptions.getBAMFilePath());
      return p.apply(
          Create.of(
              Reader.readSequentiallyForTesting(
                  GCSOptions.Methods.createStorageClient(pipelineOptions, auth),
                  pipelineOptions.getBAMFilePath(),
                  contigs.iterator().next(),
                  readerOptions)));
    }
  }
}
