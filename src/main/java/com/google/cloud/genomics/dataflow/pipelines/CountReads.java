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

import htsjdk.samtools.ValidationStringency;

import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.readers.ReadReader;
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
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.common.base.Strings;

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
    
    @Description("Whether to include unmapped mate pairs of mapped reads to match expectations of Picard tools.")
    @Default.Boolean(false)
    boolean isIncludeUnmapped();

    void setIncludeUnmapped(boolean newValue);

    public static class Methods {
      public static void validateOptions(Options options) {
        GCSOutputOptions.Methods.validateOptions(options);
      }
    }

  }

  private static final Logger LOG = Logger.getLogger(CountReads.class.getName());
  private static Pipeline p;
  private static Options pipelineOptions;
  private static OfflineAuth auth;
  
  public static void main(String[] args) throws GeneralSecurityException, IOException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(Options.class);
    pipelineOptions = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    Options.Methods.validateOptions(pipelineOptions);

    auth = GenomicsOptions.Methods.getGenomicsAuth(pipelineOptions);
    p = Pipeline.create(pipelineOptions);
    p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);

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
    PCollection<String> readCountText = readCount.apply(ParDo.of(new DoFn<Long, String>() {
      @Override
      public void processElement(DoFn<Long, String>.ProcessContext c) throws Exception {
        c.output(String.valueOf(c.element()));
      }
    }).named("toString"));
    readCountText.apply(TextIO.Write.to(pipelineOptions.getOutput()).named("WriteOutput").withoutSharding());

    p.run();
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

  private static PCollection<Read> getReads() throws IOException {
    if (!pipelineOptions.getBAMFilePath().isEmpty()) {
      return getReadsFromBAMFile();
    }
    if (!pipelineOptions.getReadGroupSetId().isEmpty()) {
      return getReadsFromAPI();
    }
    throw new IOException("Either BAM file or ReadGroupSet must be specified");
  }

  private static PCollection<Read> getReadsFromAPI() {
    List<SearchReadsRequest> requests =
        ShardUtils.getPaginatedReadRequests(Collections.singletonList(pipelineOptions.getReadGroupSetId()),
        pipelineOptions.getReferences(), pipelineOptions.getBasesPerShard());
    PCollection<SearchReadsRequest> readRequests = p.begin()
        .apply(Create.of(requests));
    PCollection<Read> reads =
        readRequests.apply(
            ParDo.of(
                new ReadReader(auth, ShardBoundary.Requirement.STRICT))
                .named(ReadReader.class.getSimpleName()));
    return reads;
  }

  private static PCollection<Read> getReadsFromBAMFile() throws IOException {
    LOG.info("getReadsFromBAMFile");

    final Iterable<Contig> contigs = Contig.parseContigsFromCommandLine(pipelineOptions.getReferences());
    final ReaderOptions readerOptions = new ReaderOptions(
        ValidationStringency.LENIENT,
        pipelineOptions.isIncludeUnmapped());
    if (pipelineOptions.isShardBAMReading()) {
      LOG.info("Sharded reading of "+ pipelineOptions.getBAMFilePath());
      return ReadBAMTransform.getReadsFromBAMFilesSharded(p,
          auth,
          contigs,
          readerOptions,
          pipelineOptions.getBAMFilePath(),
          ShardingPolicy.BYTE_SIZE_POLICY);
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
