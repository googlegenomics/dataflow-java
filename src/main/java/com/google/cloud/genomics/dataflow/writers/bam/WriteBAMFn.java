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
package com.google.cloud.genomics.dataflow.writers.bam;

import com.google.api.services.storage.Storage;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.functions.KeyReadsFn;
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

  private BoundedWindow window;

  public WriteBAMFn(final PCollectionView<HeaderInfo> headerView) {
    this.headerView = headerView;
  }

  @StartBundle
  public void startBundle(DoFn<Read, String>.StartBundleContext c) throws IOException {
    LOG.info("Starting bundle ");
    storage = Transport.newStorageClient(c.getPipelineOptions().as(GCSOptions.class)).build().objects();

    Metrics.counter(WriteBAMFn.class, "Initialized Write Shard Count").inc();
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

  @FinishBundle
  public void finishBundle(DoFn<Read, String>.FinishBundleContext c) throws IOException {
    bw.close();
    Metrics.distribution(WriteBAMFn.class, "Maximum Write Shard Processing Time (sec)")
        .update(stopWatch.elapsed(TimeUnit.SECONDS));
    LOG.info("Finished writing " + shardContig);
    Metrics.counter(WriteBAMFn.class, "Finished Write Shard Count").inc();
    final long bytesWritten = ts.getBytesWrittenExceptingTruncation();
    LOG.info("Wrote " + readCount + " reads, " + unmappedReadCount + " unmapped, into " + shardName +
        (hadOutOfOrder ? "ignored out of order" : "") + ", wrote " + bytesWritten + " bytes");
    Metrics.counter(WriteBAMFn.class, "Written reads").inc(readCount);
    Metrics.counter(WriteBAMFn.class, "Written unmapped reads").inc(unmappedReadCount);
    final long totalReadCount = (long)readCount + (long)unmappedReadCount;
    Metrics.distribution(WriteBAMFn.class, "Maximum Reads Per Shard").update(totalReadCount);
    c.output(shardName, window.maxTimestamp(), window);
    c.output(SEQUENCE_SHARD_SIZES_TAG, KV.of(sequenceIndex, bytesWritten),
        window.maxTimestamp(), window);
  }

  @ProcessElement
  public void processElement(DoFn<Read, String>.ProcessContext c, BoundedWindow window)
      throws Exception {

    this.window = window;

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
      Metrics.counter(WriteBAMFn.class, "Out of order reads").inc();
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
