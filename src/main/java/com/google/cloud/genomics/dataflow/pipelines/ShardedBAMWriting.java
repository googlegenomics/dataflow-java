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
import com.google.api.services.storage.Storage.Objects.Compose;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.ComposeRequest.SourceObjects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DelegateCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.readers.bam.BAMIO;
import com.google.cloud.genomics.dataflow.readers.bam.ReadBAMTransform;
import com.google.cloud.genomics.dataflow.readers.bam.ReaderOptions;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.TruncatedOutputStream;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.ReadUtils;
import com.google.common.collect.Lists;

import htsjdk.samtools.BAMBlockWriter;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import htsjdk.samtools.util.StringLineReader;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.channels.Channels;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.logging.Logger;

/**
 * Demonstrates sharded BAM writing
 */
public class ShardedBAMWriting {
  private static final Logger LOG = Logger.getLogger(ShardedBAMWriting.class.getName());
  private static ShardedBAMWritingOptions options;
  private static Pipeline p;
  private static GenomicsFactory.OfflineAuth auth;
  private static Iterable<Contig> contigs;

  public static interface ShardedBAMWritingOptions extends GenomicsDatasetOptions, GCSOptions {
    @Description("The Google Cloud Storage path to the BAM file to get reads data from")
    @Default.String("")
    String getBAMFilePath();

    void setBAMFilePath(String filePath);
  }

  public static void main(String[] args) throws GeneralSecurityException, IOException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(ShardedBAMWritingOptions.class);
    options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ShardedBAMWritingOptions.class);
    // Option validation is not yet automatic, we make an explicit call here.
    GenomicsDatasetOptions.Methods.validateOptions(options);
    auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    p = Pipeline.create(options);
    // Register coders
    DataflowWorkarounds.registerGenomicsCoders(p);
    DataflowWorkarounds.registerCoder(p, Contig.class, CONTIG_CODER);
    // Get contigs
    contigs = Contig.parseContigsFromCommandLine(options.getReferences());
    // Get header info
    final HeaderInfo headerInfo = getHeader();
    
    // Get the reads and shard them
    final PCollection<Read> reads = getReadsFromBAMFile();
    final PCollection<KV<Contig,Iterable<Read>>> shardedReads = ShardReadsTransform.shard(reads);
    final PCollection<String> writtenShards = WriteReadsTransform.write(shardedReads, headerInfo);
    writtenShards
        .apply(
            TextIO.Write
              .to(options.getOutput() + "-result")
        .named("Write Output Result")
        .withoutSharding());
    p.run();            
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
    while (recordIterator.hasNext()) {
      SAMRecord record = recordIterator.next();
      final int alignmentStart = record.getAlignmentStart();
      if (firstShard == null && alignmentStart > firstContig.start && alignmentStart < firstContig.end) {
        firstShard = shardFromAlignmentStart(firstContig.referenceName, alignmentStart); 
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
  
  private static PCollection<Read> getReadsFromBAMFile() {
    LOG.info("Sharded reading of "+ options.getBAMFilePath());
    
    final ReaderOptions readerOptions = new ReaderOptions(
        ValidationStringency.DEFAULT_STRINGENCY,
        true);
   
    return ReadBAMTransform.getReadsFromBAMFilesSharded(p,
        auth,
        contigs,
        readerOptions,
        Collections.singletonList(options.getBAMFilePath()));
  }
  
  public static class ShardReadsTransform extends PTransform<PCollection<Read>, 
      PCollection<KV<Contig, Iterable<Read>>>> {
    @Override
    public PCollection<KV<Contig, Iterable<Read>>> apply(PCollection<Read> reads) {
      return reads
        .apply(ParDo.named("KeyReads").of(new KeyReadsFn()))
        .apply(GroupByKey.<Contig, Read>create());
    }
    
    public static PCollection<KV<Contig, Iterable<Read>>> shard(PCollection<Read> reads) {
      return (new ShardReadsTransform()).apply(reads);
    }
  } 

  public static class KeyReadsFn extends DoFn<Read, KV<Contig,Read>> {
    @Override
    public void processElement(DoFn<Read, KV<Contig, Read>>.ProcessContext c)
        throws Exception {
      final Read read = c.element();
      c.output(KV.of(shardKeyForRead(read), read));
    }
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
  
  static final SAMTextHeaderCodec SAM_HEADER_CODEC = new SAMTextHeaderCodec();
  static {
    SAM_HEADER_CODEC.setValidationStringency(ValidationStringency.SILENT);
  }
  
  static Coder<HeaderInfo> HEADER_INFO_CODER = DelegateCoder.of(
      StringUtf8Coder.of(),
      new DelegateCoder.CodingFunction<HeaderInfo,String>() {
        @Override
        public String apply(HeaderInfo info) throws Exception {
          final StringWriter stringWriter = new StringWriter();
          SAM_HEADER_CODEC.encode(stringWriter, info.header);
          return info.firstShard.toString() + "\n" + stringWriter.toString();
        }
      },
      new DelegateCoder.CodingFunction<String, HeaderInfo>() {
        @Override
        public HeaderInfo apply(String str) throws Exception {
          int newLinePos = str.indexOf("\n");
          String contigStr = str.substring(0, newLinePos);
          String headerStr = str.substring(newLinePos + 1);
          return new HeaderInfo(
              SAM_HEADER_CODEC.decode(new StringLineReader(headerStr), 
                  "HEADER_INFO_CODER"),
              Contig.parseContigsFromCommandLine(contigStr).iterator().next());
        }
      });

  static final long LOCI_PER_SHARD = 10000;
  
  static Contig shardKeyForRead(Read read) {
    String referenceName = null;
    Long alignmentStart = null;
    if (read.getAlignment() != null) {
      if (read.getAlignment().getPosition() != null ) {
        referenceName = read.getAlignment().getPosition().getReferenceName();
        alignmentStart = read.getAlignment().getPosition().getPosition();
      }
    }
    // If this read is unmapped but its mate is mapped, group them together
    if (referenceName == null || alignmentStart == null) {
      if (read.getNextMatePosition() != null) {
        referenceName = read.getNextMatePosition().getReferenceName();
        alignmentStart = read.getNextMatePosition().getPosition();
      }
    }  
    if (referenceName == null || alignmentStart == null) { 
      referenceName = "*";
      alignmentStart = new Long(0);
    }
    return shardFromAlignmentStart(referenceName, alignmentStart);
  }
  
  static Contig shardFromAlignmentStart(String referenceName, long alignmentStart) {
    final long shardStart = (alignmentStart / LOCI_PER_SHARD) * LOCI_PER_SHARD;
    return new Contig(referenceName, shardStart, shardStart + LOCI_PER_SHARD);
  }
  
  public static TupleTag<KV<Contig, Iterable<Read>>> SHARDED_READS_TAG = new TupleTag<>();
  public static TupleTag<HeaderInfo> HEADER_TAG = new TupleTag<>();
  
  public static class WriteReadsTransform 
    extends PTransform<PCollectionTuple, PCollection<String>> {
   
    @Override
    public PCollection<String> apply(PCollectionTuple tuple) {
      final PCollection<HeaderInfo> header = tuple.get(HEADER_TAG);
      final PCollectionView<HeaderInfo> headerView =
          header.apply(View.<HeaderInfo>asSingleton());
      
      final PCollection<KV<Contig, Iterable<Read>>> shardedReads = tuple.get(SHARDED_READS_TAG);
      
      final PCollection<String> writtenShardNames = 
          shardedReads.apply(ParDo.named("Write shards")
            .withSideInputs(Arrays.asList(headerView))
            .of(new WriteShardFn(headerView)));
      
      final PCollectionView<Iterable<String>> writtenShardsView = 
          writtenShardNames.apply(View.<String>asIterable());
      
      final PCollection<String> destinationPath = p.apply(
          Create.<String>of(options.getOutput()));
      
      final PCollection<String> writtenFile = destinationPath.apply(
          ParDo.named("Combine shards")
            .withSideInputs(writtenShardsView)
            .of(new CombineShardsFn(writtenShardsView)));
      
      return writtenFile;
    }
    
    public static PCollection<String> write(PCollection<KV<Contig, Iterable<Read>>> shardedReads, HeaderInfo headerInfo) {
      final PCollectionTuple tuple = PCollectionTuple
          .of(SHARDED_READS_TAG,shardedReads)
          .and(HEADER_TAG, p.apply(Create.of(headerInfo))
             .setCoder(HEADER_INFO_CODER));
      return (new WriteReadsTransform()).apply(tuple);
    }
  }
  
  public static class WriteShardFn extends DoFn<KV<Contig, Iterable<Read>>, String> {
    final PCollectionView<HeaderInfo> headerView;
    Storage.Objects storage;
    
    public WriteShardFn(final PCollectionView<HeaderInfo> headerView) {
      this.headerView = headerView;
    }
    
    @Override
    public void startBundle(DoFn<KV<Contig, Iterable<Read>>, String>.Context c) throws IOException {
      storage = Transport.newStorageClient(c.getPipelineOptions().as(GCSOptions.class)).build().objects();
    }
    
    @Override
    public void processElement(DoFn<KV<Contig, Iterable<Read>>, String>.ProcessContext c)
        throws Exception {
      final HeaderInfo headerInfo = c.sideInput(headerView);
      final KV<Contig, Iterable<Read>> shard = c.element();
      final Contig shardContig = shard.getKey();
      final Iterable<Read> reads = shard.getValue();
      final boolean isFirstShard = shardContig.equals(headerInfo.firstShard);
      if (isFirstShard) {
        LOG.info("Writing first shard " + shardContig);
      } else {
        LOG.info("Writing non-first shard " + shardContig);
      }
      
      final String writeResult = writeShard(headerInfo.header, 
          shardContig, reads, c.getPipelineOptions().as(ShardedBAMWritingOptions.class),
          isFirstShard);
      c.output(writeResult);
      LOG.info("Finished writing " + shardContig);
    }
    
    String writeShard(SAMFileHeader header, Contig shardContig, Iterable<Read> reads, 
        ShardedBAMWritingOptions options, boolean isFirstShard) throws IOException {
      final String outputFileName = options.getOutput();
      final String shardName = outputFileName + "-" + shardContig;
      LOG.info("Writing shard file " + shardName);
      final OutputStream outputStream = 
          Channels.newOutputStream(
              new GcsUtil.GcsUtilFactory().create(options)
                .create(GcsPath.fromUri(shardName), 
                    "application/octet-stream"));
      int count = 0;
      // Use a TruncatedOutputStream to avoid writing the empty gzip block that
      // indicates EOF.
      final BAMBlockWriter bw = new BAMBlockWriter(new TruncatedOutputStream(
          outputStream, BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK.length),
          null /*file*/);
      bw.setSortOrder(header.getSortOrder(), true /*presorted*/);
      bw.setHeader(header);
      if (isFirstShard) {
        bw.writeHeader(header);
      }
      for (Read read : reads) {
        SAMRecord samRecord = ReadUtils.makeSAMRecord(read, header);
        bw.addAlignment(samRecord);
        count++;
      }
      bw.close();
      LOG.info("Wrote " + count + " reads into " + shardName); 
      return shardName;
    }
  }
  
  public static class CombineShardsFn extends DoFn<String, String> {
    final PCollectionView<Iterable<String>> shards;
    
    public CombineShardsFn(PCollectionView<Iterable<String>> shards) {
      this.shards= shards;
    }
    
    @Override
    public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
      final String result = 
          combineShards(
              c.getPipelineOptions().as(ShardedBAMWritingOptions.class), 
              c.element(),
              c.sideInput(shards));
      c.output(result);
    }

    static String combineShards(ShardedBAMWritingOptions options, String dest,
        Iterable<String> shards) throws IOException {
      LOG.info("Combining shards into " + dest);
      final Storage.Objects storage = Transport.newStorageClient(
          options
            .as(GCSOptions.class))
            .build()
            .objects();
      
      final GcsPath destPath = GcsPath.fromUri(dest);
      
      StorageObject destination = new StorageObject()
        .setContentType("application/octet-stream");
      
      ArrayList<String> sortedShardsNames = Lists.newArrayList(shards);
      Collections.sort(sortedShardsNames);

      // Write an EOF block (empty gzip block), and put it at the end.
      String eofFileName = options.getOutput() + "-EOF";
      final OutputStream os = Channels.newOutputStream(
          (new GcsUtil.GcsUtilFactory()).create(options).create(
              GcsPath.fromUri(eofFileName),
          "application/octet-stream"));
      os.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
      os.close();
      sortedShardsNames.add(eofFileName);
      
      // list of files to concatenate
      ArrayList<SourceObjects> sourceObjects = new ArrayList<SourceObjects>();
      for (String shard : sortedShardsNames) {
          final GcsPath shardPath = GcsPath.fromUri(shard);
          LOG.info("Adding object " + shardPath);
          sourceObjects.add( new SourceObjects().setName(shardPath.getObject()) );
      }
  
      final ComposeRequest composeRequest = new ComposeRequest()
        .setDestination(destination)
        .setSourceObjects(sourceObjects);
      final Compose compose = storage.compose(
          destPath.getBucket(), destPath.getObject(), composeRequest); 
      final String combineResult =  compose.execute().toString(); 
      LOG.info("Combine result is " + combineResult);
      return combineResult;
    }
  }
}