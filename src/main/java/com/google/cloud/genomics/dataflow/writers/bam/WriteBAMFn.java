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
package com.google.cloud.genomics.dataflow.functions;

import com.google.api.services.storage.Storage;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Max;
import com.google.cloud.dataflow.sdk.transforms.Min;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.Sum.SumIntegerFn;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.readers.bam.BAMIO;
import com.google.cloud.genomics.dataflow.readers.bam.HeaderInfo;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GCSOutputOptions;
import com.google.cloud.genomics.dataflow.utils.TruncatedOutputStream;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.grpc.ReadUtils;
import com.google.common.base.Stopwatch;
import com.google.genomics.v1.Read;

import htsjdk.samtools.BAMBlockWriter;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.BlockCompressedStreamConstants;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/*
 * Writes a PCollection of Reads to a BAM file.
 * Assumes sharded execution and writes each bundle as a separate BAM file, outputting
 * its name at the end of the bundle.
 */
public class WriteBAMFn extends DoFn<Read, String> {

  public static interface Options extends GCSOutputOptions {}

  private static final Logger LOG = Logger.getLogger(WriteBAMFn.class.getName());
  public static TupleTag<String> WRITTEN_BAM_NAMES_TAG = new TupleTag<String>(){};
  public static TupleTag<KV<Integer, Long>> SEQUENCE_SHARD_SIZES_TAG = new TupleTag<KV<Integer, Long>>(){};

  final PCollectionView<HeaderInfo> headerView;
  Storage.Objects storage;
  Aggregator<Integer, Integer> readCountAggregator;
  Aggregator<Integer, Integer> unmappedReadCountAggregator;
  Aggregator<Integer, Integer> initializedShardCount;
  Aggregator<Integer, Integer> finishedShardCount;
  Aggregator<Long, Long> shardTimeMaxSec;
  Aggregator<Long, Long> shardReadCountMax;
  Aggregator<Long, Long> shardReadCountMin;
  Aggregator<Integer, Integer> outOfOrderCount;
  
  Stopwatch stopWatch;
  int readCount;
  int unmappedReadCount;
  String shardName;
  TruncatedOutputStream ts;
  BAMBlockWriter bw;
  Contig shardContig;
  Options options;
  HeaderInfo headerInfo;
  int sequenceIndex;
  
  SAMRecord prevRead = null;
  long minAlignment = Long.MAX_VALUE;
  long maxAlignment = Long.MIN_VALUE;
  boolean hadOutOfOrder = false;

  public WriteBAMFn(final PCollectionView<HeaderInfo> headerView) {
    this.headerView = headerView;
    readCountAggregator = createAggregator("Written reads", new SumIntegerFn());
    unmappedReadCountAggregator = createAggregator("Written unmapped reads", new SumIntegerFn());
    initializedShardCount = createAggregator("Initialized Write Shard Count", new Sum.SumIntegerFn());
    finishedShardCount = createAggregator("Finished Write Shard Count", new Sum.SumIntegerFn());
    shardTimeMaxSec = createAggregator("Maximum Write Shard Processing Time (sec)", new Max.MaxLongFn());
    shardReadCountMax = createAggregator("Maximum Reads Per Shard", new Max.MaxLongFn());
    shardReadCountMin = createAggregator("Minimum Reads Per Shard", new Min.MinLongFn());
    outOfOrderCount  = createAggregator("Out of order reads",  new Sum.SumIntegerFn());
  }
  
  @Override
  public void startBundle(DoFn<Read, String>.Context c) throws IOException {
    LOG.info("Starting bundle ");
    storage = Transport.newStorageClient(c.getPipelineOptions().as(GCSOptions.class)).build().objects();
    
    initializedShardCount.addValue(1);
    stopWatch = Stopwatch.createStarted();
    
    options = c.getPipelineOptions().as(Options.class);
    
    readCount = 0;
    unmappedReadCount = 0;
    headerInfo = null;
    prevRead = null;
    minAlignment = Long.MAX_VALUE;
    maxAlignment = Long.MIN_VALUE;
    hadOutOfOrder = false;
  }
  
  @Override
  public void finishBundle(DoFn<Read, String>.Context c) throws IOException {
    bw.close();
    shardTimeMaxSec.addValue(stopWatch.elapsed(TimeUnit.SECONDS));
    LOG.info("Finished writing " + shardContig);
    finishedShardCount.addValue(1);
    final long bytesWritten = ts.getBytesWrittenExceptingTruncation();
    LOG.info("Wrote " + readCount + " reads, " + unmappedReadCount + " unmapped, into " + shardName + 
        (hadOutOfOrder ? "ignored out of order" : "") + ", wrote " + bytesWritten + " bytes");
    readCountAggregator.addValue(readCount);
    unmappedReadCountAggregator.addValue(unmappedReadCount);
    final long totalReadCount = (long)readCount + (long)unmappedReadCount;
    shardReadCountMax.addValue(totalReadCount);
    shardReadCountMin.addValue(totalReadCount);
    c.output(shardName);
    c.sideOutput(SEQUENCE_SHARD_SIZES_TAG, KV.of(sequenceIndex, bytesWritten));
  }
  
  @Override
  public void processElement(DoFn<Read, String>.ProcessContext c)
      throws Exception {
   
    if (headerInfo == null) {
      headerInfo = c.sideInput(headerView);
    }
    final Read read = c.element();
    
    if (readCount == 0) {
      
      shardContig = KeyReadsFn.shardKeyForRead(read, 1);
      sequenceIndex = headerInfo.header.getSequenceIndex(shardContig.referenceName);
      final boolean isFirstShard = headerInfo.shardHasFirstRead(shardContig);
      final String outputFileName = options.getOutput();
      shardName = outputFileName + "-" + String.format("%012d", sequenceIndex) + "-"
          + shardContig.referenceName
          + ":" + String.format("%012d", shardContig.start);
      LOG.info("Writing shard file " + shardName);
      final OutputStream outputStream = 
          Channels.newOutputStream(
              new GcsUtil.GcsUtilFactory().create(options)
                .create(GcsPath.fromUri(shardName), 
                    BAMIO.BAM_INDEX_FILE_MIME_TYPE));
      ts = new TruncatedOutputStream(
          outputStream, BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK.length);
      bw = new BAMBlockWriter(ts, null /*file*/);
      bw.setSortOrder(headerInfo.header.getSortOrder(), true);
      bw.setHeader(headerInfo.header);
      if (isFirstShard) {
        LOG.info("First shard - writing header to " + shardName);
        bw.writeHeader(headerInfo.header);
      }
    }
    SAMRecord samRecord = ReadUtils.makeSAMRecord(read, headerInfo.header);
    if (prevRead != null && prevRead.getAlignmentStart() > samRecord.getAlignmentStart()) {
      LOG.info("Out of order read " + prevRead.getAlignmentStart() + " " + 
          samRecord.getAlignmentStart() + " during writing of shard " + shardName + 
          " after processing " + readCount + " reads, min seen alignment is " + 
          minAlignment + " and max is " + maxAlignment + ", this read is " + 
          (samRecord.getReadUnmappedFlag() ? "unmapped" : "mapped") + " and its mate is " + 
          (samRecord.getMateUnmappedFlag() ? "unmapped" : "mapped"));
      outOfOrderCount.addValue(1);
      readCount++;
      hadOutOfOrder = true;
      return;
    }
    minAlignment = Math.min(minAlignment, samRecord.getAlignmentStart());
    maxAlignment = Math.max(maxAlignment, samRecord.getAlignmentStart());
    prevRead = samRecord;
    if (samRecord.getReadUnmappedFlag()) {
      if (!samRecord.getMateUnmappedFlag()) {
        samRecord.setReferenceName(samRecord.getMateReferenceName());
        samRecord.setAlignmentStart(samRecord.getMateAlignmentStart());
      }
      unmappedReadCount++;
    }
    bw.addAlignment(samRecord);
    readCount++;
  }
}