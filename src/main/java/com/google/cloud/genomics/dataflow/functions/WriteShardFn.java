package com.google.cloud.genomics.dataflow.functions;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;

import com.google.api.services.genomics.model.Read;
import com.google.api.services.storage.Storage;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum.SumIntegerFn;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import com.google.cloud.genomics.dataflow.pipelines.ShardedBAMWriting.HeaderInfo;

import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.utils.ReadUtils;
import com.google.cloud.genomics.dataflow.utils.ShardedBAMWritingOptions;
import com.google.cloud.genomics.dataflow.utils.TruncatedOutputStream;

import com.google.cloud.genomics.utils.Contig;

import htsjdk.samtools.BAMBlockWriter;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.logging.Logger;

public class WriteShardFn extends DoFn<KV<Contig, Iterable<Read>>, String> {
  private static final int MAX_RETRIES_FOR_WRITING_A_SHARD = 4;
  private static final String BAM_INDEX_FILE_MIME_TYPE = "application/octet-stream";
  private static final Logger LOG = Logger.getLogger(WriteShardFn.class.getName());

  final PCollectionView<HeaderInfo> headerView;
  Storage.Objects storage;
  Aggregator<Integer, Integer> readCountAggregator;
  Aggregator<Integer, Integer> unmappedReadCountAggregator;

  public WriteShardFn(final PCollectionView<HeaderInfo> headerView) {
    this.headerView = headerView;
    readCountAggregator = createAggregator("Written reads", new SumIntegerFn());
    unmappedReadCountAggregator = createAggregator("Written unmapped reads", new SumIntegerFn());
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
//      LOG.info("Writing first shard " + shardContig);
    } else {
//      LOG.info("Writing non-first shard " + shardContig);
    }

    int numRetriesLeft = MAX_RETRIES_FOR_WRITING_A_SHARD;
    boolean done = false;
    do {
     try {
      final String writeResult = writeShard(headerInfo.header,
          shardContig, reads,
          c.getPipelineOptions().as(ShardedBAMWritingOptions.class),
          isFirstShard);
      c.output(writeResult);
      done = true;
     } catch (IOException iox) {
       LOG.warning("Write shard failed for " + shardContig + ": " + iox.getMessage());
       if (--numRetriesLeft <= 0) {
         LOG.warning("No more retries - failing the task for " + shardContig);
         throw iox;
       }
     }
    } while (!done);
    LOG.info("Finished writing " + shardContig);
  }
  
  String writeShard(SAMFileHeader header, Contig shardContig, Iterable<Read> reads, 
      ShardedBAMWritingOptions options, boolean isFirstShard) throws IOException {
    final String outputFileName = options.getOutput();
    final String shardName = outputFileName + "-" + shardContig.referenceName
        + ":" + String.format("%012d", shardContig.start) + "-" + 
        String.format("%012d", shardContig.end);
    LOG.info("Writing shard file " + shardName);
    final OutputStream outputStream = 
        Channels.newOutputStream(
            new GcsUtil.GcsUtilFactory().create(options)
              .create(GcsPath.fromUri(shardName), 
                  BAM_INDEX_FILE_MIME_TYPE));
    int count = 0;
    int countUnmapped = 0;
    // Use a TruncatedOutputStream to avoid writing the empty gzip block that
    // indicates EOF.
    final BAMBlockWriter bw = new BAMBlockWriter(new TruncatedOutputStream(
        outputStream, BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK.length),
        null /*file*/);
    // If reads are unsorted then we do not care about their order
    // otherwise we need to sort them as we write.
    final boolean treatReadsAsPresorted = 
        header.getSortOrder() == SAMFileHeader.SortOrder.unsorted;
    bw.setSortOrder(header.getSortOrder(), treatReadsAsPresorted);
    bw.setHeader(header);
    if (isFirstShard) {
      LOG.info("First shard - writing header to " + shardName);
      bw.writeHeader(header);
    }
    for (Read read : reads) {
      SAMRecord samRecord = ReadUtils.makeSAMRecord(read, header);
      if (samRecord.getReadUnmappedFlag()) {
        if (!samRecord.getMateUnmappedFlag()) {
          samRecord.setReferenceName(samRecord.getMateReferenceName());
          samRecord.setAlignmentStart(samRecord.getMateAlignmentStart());
        }
        countUnmapped++;
      }
      bw.addAlignment(samRecord);
      count++;
    }
    bw.close();
    LOG.info("Wrote " + count + " reads, " + countUnmapped + " umapped, into " + shardName);
    readCountAggregator.addValue(count);
    unmappedReadCountAggregator.addValue(countUnmapped);
    return shardName;
  }
}