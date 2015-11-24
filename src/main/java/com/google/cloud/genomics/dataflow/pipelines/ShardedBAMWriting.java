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

import com.google.api.services.genomics.model.Read;
import com.google.api.services.storage.Storage;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DelegateCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.readers.bam.BAMIO;
import com.google.cloud.genomics.dataflow.readers.bam.ReadBAMTransform;
import com.google.cloud.genomics.dataflow.readers.bam.ReaderOptions;
import com.google.cloud.genomics.dataflow.readers.bam.ShardingPolicy;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardReadsTransform;
import com.google.cloud.genomics.dataflow.writers.WriteReadsTransform;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.common.collect.Lists;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.ValidationStringency;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.logging.Logger;

/**
 * Demonstrates loading some Reads, sharding them, writing them to various BAM files in parallel,
 * then combining the shards and writing an index for the combined BAM file.
 */
public class ShardedBAMWriting {

  static interface Options extends ShardReadsTransform.Options, WriteReadsTransform.Options {
    @Description("The Google Cloud Storage path to the BAM file to get reads data from")
    @Default.String("")
    String getBAMFilePath();

    void setBAMFilePath(String filePath);
  }

  private static final Logger LOG = Logger.getLogger(ShardedBAMWriting.class.getName());
  private static final String BAM_INDEX_FILE_MIME_TYPE = "application/octet-stream";
  private static final int MAX_FILES_FOR_COMPOSE = 32;
  private static Options options;
  private static Pipeline pipeline;
  private static GenomicsFactory.OfflineAuth auth;
  private static Iterable<Contig> contigs;

  public static void main(String[] args) throws GeneralSecurityException, IOException {
    // Register the options so that they show up via --help.
    PipelineOptionsFactory.register(Options.class);
    options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    GenomicsDatasetOptions.Methods.validateOptions(options);
    auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    pipeline = Pipeline.create(options);
    // Register coders.
    pipeline.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);
    pipeline.getCoderRegistry().registerCoder(Contig.class, CONTIG_CODER);
    // Process options.
    contigs = Contig.parseContigsFromCommandLine(options.getReferences());
    // Get header info.
    final HeaderInfo headerInfo = getHeader();
    
    // Get the reads and shard them.
    final PCollection<Read> reads = getReadsFromBAMFile();
    final PCollection<KV<Contig,Iterable<Read>>> shardedReads = ShardReadsTransform.shard(reads);
    final PCollection<String> writtenShards = WriteReadsTransform.write(
        shardedReads, headerInfo, options.getOutput(), pipeline);
    writtenShards
        .apply(
            TextIO.Write
              .to(options.getOutput() + "-result")
        .named("Write Output Result")
        .withoutSharding());
    pipeline.run();            
  }
 
  public static class HeaderInfo {
    public SAMFileHeader header;
    public Contig firstShard;
    
    public HeaderInfo(SAMFileHeader header, Contig firstShard) {
      this.header = header;
      this.firstShard = firstShard;
    }
  }
  
  private static HeaderInfo getHeader() throws IOException {
    HeaderInfo result = null;
    
    // Get first contig
    final ArrayList<Contig> contigsList = Lists.newArrayList(contigs);
    if (contigsList.size() <= 0) {
      throw new IOException("No contigs specified");
    }
    Collections.sort(contigsList, new Comparator<Contig>() {
      @Override
      public int compare(Contig o1, Contig o2) {
        int compRefs =  o1.referenceName.compareTo(o2.referenceName);
        if (compRefs != 0) {
          return compRefs;
        }
        return (int)(o1.start - o2.start);
      }
    });
    final Contig firstContig = contigsList.get(0);
    
    // Open and read start of BAM
    final Storage.Objects storage = Transport.newStorageClient(
        options
          .as(GCSOptions.class))
          .build()
          .objects();
    LOG.info("Reading header from " + options.getBAMFilePath());
    final SamReader samReader = BAMIO
        .openBAM(storage, options.getBAMFilePath(), ValidationStringency.DEFAULT_STRINGENCY);
    final SAMFileHeader header = samReader.getFileHeader();
    
    LOG.info("Reading first chunk of reads from " + options.getBAMFilePath());
    final SAMRecordIterator recordIterator = samReader.query(
        firstContig.referenceName, (int)firstContig.start + 1, (int)firstContig.end + 1, false);
   
    Contig firstShard = null;
    while (recordIterator.hasNext() && result == null) {
      SAMRecord record = recordIterator.next();
      final int alignmentStart = record.getAlignmentStart();
      if (firstShard == null && alignmentStart > firstContig.start && alignmentStart < firstContig.end) {
        firstShard = shardFromAlignmentStart(firstContig.referenceName, alignmentStart, options.getLociPerWritingShard());
        LOG.info("Determined first shard to be " + firstShard);
        result = new HeaderInfo(header, firstShard);
      }
    }
    recordIterator.close();
    samReader.close();
    
    if (result == null) {
      throw new IOException("Did not find reads for the first contig " + firstContig.toString());
    }
    LOG.info("Finished header reading from " + options.getBAMFilePath());
    return result;
  }

  /**
   * Policy used to shard Reads.
   * By default we are using the default sharding supplied by the policy class.
   * If you want custom sharding, use the following pattern:
   * <pre>
   *    READ_SHARDING_POLICY = new ShardingPolicy() {
   *     @Override
   *     public boolean shardBigEnough(BAMShard shard) {
   *       return shard.sizeInLoci() > 50000000;
   *     }
   *   };
   * </pre>
   */
  private static final ShardingPolicy READ_SHARDING_POLICY = ShardingPolicy.BYTE_SIZE_POLICY;
      
  private static PCollection<Read> getReadsFromBAMFile() throws IOException {
    LOG.info("Sharded reading of "+ options.getBAMFilePath());
    
    final ReaderOptions readerOptions = new ReaderOptions(
        ValidationStringency.DEFAULT_STRINGENCY,
        true);
   
    return ReadBAMTransform.getReadsFromBAMFilesSharded(pipeline,
        auth,
        contigs,
        readerOptions,
        options.getBAMFilePath(),
        READ_SHARDING_POLICY);
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
        public Contig apply(String str) throws Exception {
          return Contig.parseContigsFromCommandLine(str).iterator().next();
        }
      });

  static Contig shardFromAlignmentStart(String referenceName, long alignmentStart, long lociPerShard) {
    final long shardStart = (alignmentStart / lociPerShard) * lociPerShard;
    return new Contig(referenceName, shardStart, shardStart + lociPerShard);
  }
}
