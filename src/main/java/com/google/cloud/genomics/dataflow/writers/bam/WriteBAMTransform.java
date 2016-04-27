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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DelegateCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.google.cloud.genomics.dataflow.functions.BreakFusionTransform;
import com.google.cloud.genomics.dataflow.functions.GetReferencesFromHeaderFn;
import com.google.cloud.genomics.dataflow.readers.bam.HeaderInfo;
import com.google.cloud.genomics.utils.Contig;
import com.google.genomics.v1.Read;

import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import htsjdk.samtools.util.StringLineReader;

import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/*
 * Writes sets of reads to BAM files in parallel, then combines the files and writes an index
 * for the combined file.
 */
public class WriteBAMTransform extends PTransform<PCollectionTuple, PCollection<String>> {

  public static interface Options extends WriteBAMFn.Options {}

  public static TupleTag<Read> SHARDED_READS_TAG = new TupleTag<Read>(){};
  public static TupleTag<HeaderInfo> HEADER_TAG = new TupleTag<HeaderInfo>(){};

  private String output;
  private Pipeline pipeline;

  @Override
  public PCollection<String> apply(PCollectionTuple tuple) {
    final PCollection<HeaderInfo> header = tuple.get(HEADER_TAG);
    final PCollectionView<HeaderInfo> headerView =
        header.apply(View.<HeaderInfo>asSingleton());

    final PCollection<Read> shardedReads = tuple.get(SHARDED_READS_TAG);

    final PCollectionTuple writeBAMFilesResult =
        shardedReads.apply(ParDo.named("Write BAM shards")
          .withSideInputs(Arrays.asList(headerView))
          .withOutputTags(WriteBAMFn.WRITTEN_BAM_NAMES_TAG, TupleTagList.of(WriteBAMFn.SEQUENCE_SHARD_SIZES_TAG))
          .of(new WriteBAMFn(headerView)));

    PCollection<String> writtenBAMShardNames = writeBAMFilesResult.get(WriteBAMFn.WRITTEN_BAM_NAMES_TAG);
    final PCollectionView<Iterable<String>> writtenBAMShardsView =
        writtenBAMShardNames.apply(View.<String>asIterable());

    final PCollection<KV<Integer, Long>> sequenceShardSizes = writeBAMFilesResult.get(WriteBAMFn.SEQUENCE_SHARD_SIZES_TAG);
    final PCollection<KV<Integer, Long>> sequenceShardSizesCombined = sequenceShardSizes.apply(
        Combine.<Integer, Long, Long>perKey(
            new Sum.SumLongFn()));
    final PCollectionView<Iterable<KV<Integer, Long>>> sequenceShardSizesView =
        sequenceShardSizesCombined.apply(View.<KV<Integer, Long>>asIterable());

    final PCollection<String> destinationBAMPath = this.pipeline.apply(
        Create.<String>of(this.output));

    final PCollectionView<byte[]> eofForBAM = pipeline.apply(
        Create.<byte[]>of(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK))
        .apply(View.<byte[]>asSingleton());

    final PCollection<String> writtenBAMFile = destinationBAMPath.apply(
        ParDo.named("Combine BAM shards")
          .withSideInputs(writtenBAMShardsView, eofForBAM)
          .of(new CombineShardsFn(writtenBAMShardsView, eofForBAM)));

    final PCollectionView<String> writtenBAMFileView =
        writtenBAMFile.apply(View.<String>asSingleton());

    final PCollection<String> indexShards = header.apply(
        ParDo.named("Generate index shard tasks")
        .of(new GetReferencesFromHeaderFn()));

    final PCollectionTuple indexingResult = indexShards
        .apply(new BreakFusionTransform<String>())
        .apply(
          ParDo.named("Write index shards")
            .withSideInputs(headerView, writtenBAMFileView, sequenceShardSizesView)
            .withOutputTags(WriteBAIFn.WRITTEN_BAI_NAMES_TAG,
                TupleTagList.of(WriteBAIFn.NO_COORD_READS_COUNT_TAG))
            .of(new WriteBAIFn(headerView, writtenBAMFileView, sequenceShardSizesView)));

    final PCollection<String> writtenBAIShardNames = indexingResult.get(WriteBAIFn.WRITTEN_BAI_NAMES_TAG);
    final PCollectionView<Iterable<String>> writtenBAIShardsView =
        writtenBAIShardNames.apply(View.<String>asIterable());

    final PCollection<Long> noCoordCounts = indexingResult.get(WriteBAIFn.NO_COORD_READS_COUNT_TAG);

    final PCollection<Long> totalNoCoordCount = noCoordCounts
          .apply(new BreakFusionTransform<Long>())
          .apply(
              Combine.globally(new Sum.SumLongFn()));

    final PCollection<byte[]> totalNoCoordCountBytes = totalNoCoordCount.apply(
        ParDo.named("No coord count to bytes").of(new Long2BytesFn()));
    final PCollectionView<byte[]> eofForBAI = totalNoCoordCountBytes
        .apply(View.<byte[]>asSingleton());

    final PCollection<String> destinationBAIPath = this.pipeline.apply(
        Create.<String>of(this.output + ".bai"));

    final PCollection<String> writtenBAIFile = destinationBAIPath.apply(
        ParDo.named("Combine BAI shards")
          .withSideInputs(writtenBAIShardsView, eofForBAI)
          .of(new CombineShardsFn(writtenBAIShardsView, eofForBAI)));

    final PCollection<String> writtenFileNames = PCollectionList.of(writtenBAMFile).and(writtenBAIFile)
        .apply(Flatten.<String>pCollections());

    return writtenFileNames;
  }

  /**
   * Transforms a long value to bytes (little endian order).
   * Used for transforming the no-coord. read count into bytes for writing in
   * the footer of the BAI file.
   */
  static class Long2BytesFn extends DoFn<Long, byte[]> {
    public Long2BytesFn() {
    }

    @Override
    public void processElement(DoFn<Long, byte[]>.ProcessContext c) throws Exception {
      ByteBuffer b = ByteBuffer.allocate(8);
      b.order(ByteOrder.LITTLE_ENDIAN);
      b.putLong(c.element());
      c.output(b.array());
    }
  }

  private WriteBAMTransform(String output, Pipeline pipeline) {
    this.output = output;
    this.pipeline = pipeline;
  }

  public static PCollection<String> write(PCollection<Read> shardedReads, HeaderInfo headerInfo,
      String output, Pipeline pipeline) {
    final PCollectionTuple tuple = PCollectionTuple
        .of(SHARDED_READS_TAG,shardedReads)
        .and(HEADER_TAG, pipeline.apply(Create.of(headerInfo).withCoder(HEADER_INFO_CODER)));
    return (new WriteBAMTransform(output, pipeline)).apply(tuple);
  }

  static Coder<HeaderInfo> HEADER_INFO_CODER = DelegateCoder.of(
      StringUtf8Coder.of(),
      new DelegateCoder.CodingFunction<HeaderInfo,String>() {
        @Override
        public String apply(HeaderInfo info) throws Exception {
          final StringWriter stringWriter = new StringWriter();
          SAM_HEADER_CODEC.encode(stringWriter, info.header);
          return info.firstRead.toString() + "\n" + stringWriter.toString();
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

  static final SAMTextHeaderCodec SAM_HEADER_CODEC = new SAMTextHeaderCodec();
  static {
    SAM_HEADER_CODEC.setValidationStringency(ValidationStringency.SILENT);
  }
}