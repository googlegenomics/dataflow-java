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
package com.google.cloud.genomics.dataflow.writers;

import com.google.api.services.genomics.model.Read;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DelegateCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import com.google.cloud.genomics.dataflow.functions.CombineShardsFn;
import com.google.cloud.genomics.dataflow.functions.WriteShardFn;
import com.google.cloud.genomics.dataflow.pipelines.ShardedBAMWriting.HeaderInfo;
import com.google.cloud.genomics.utils.Contig;

import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.util.StringLineReader;
import htsjdk.samtools.ValidationStringency;

import java.io.StringWriter;
import java.util.Arrays;

/*
 * Writes sets of reads to BAM files in parallel, then combines the files and writes an index
 * for the combined file.
 */
public class WriteReadsTransform extends PTransform<PCollectionTuple, PCollection<String>> {

  public static interface Options extends WriteShardFn.Options {}

  public static TupleTag<KV<Contig, Iterable<Read>>> SHARDED_READS_TAG = new TupleTag<>();
  public static TupleTag<HeaderInfo> HEADER_TAG = new TupleTag<>();
  private String output;
  private Pipeline pipeline;

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
    
    final PCollection<String> destinationPath = this.pipeline.apply(
        Create.<String>of(this.output));
    
    final PCollection<String> writtenFile = destinationPath.apply(
        ParDo.named("Combine shards")
          .withSideInputs(writtenShardsView)
          .of(new CombineShardsFn(writtenShardsView)));
    
    return writtenFile;
  }
  
  private WriteReadsTransform(String output, Pipeline pipeline) {
    this.output = output;
    this.pipeline = pipeline;
  }
  
  public static PCollection<String> write(PCollection<KV<Contig, Iterable<Read>>> shardedReads, HeaderInfo headerInfo,
      String output, Pipeline pipeline) {
    final PCollectionTuple tuple = PCollectionTuple
        .of(SHARDED_READS_TAG,shardedReads)
        .and(HEADER_TAG, pipeline.apply(Create.of(headerInfo).withCoder(HEADER_INFO_CODER)));
    return (new WriteReadsTransform(output, pipeline)).apply(tuple);
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
  
  static final SAMTextHeaderCodec SAM_HEADER_CODEC = new SAMTextHeaderCodec();
  static {
    SAM_HEADER_CODEC.setValidationStringency(ValidationStringency.SILENT);
  }
}