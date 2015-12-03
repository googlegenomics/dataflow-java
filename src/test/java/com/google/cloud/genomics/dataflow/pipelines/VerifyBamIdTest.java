/*
 * Copyright 2015 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.genomics.dataflow.pipelines;

import com.google.api.services.genomics.model.Position;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.model.AlleleFreq;
import com.google.cloud.genomics.dataflow.model.ReadBaseQuality;
import com.google.cloud.genomics.dataflow.model.ReadCounts;
import com.google.cloud.genomics.dataflow.model.ReadQualityCount;
import com.google.cloud.genomics.dataflow.pipelines.VerifyBamId.FilterFreq;
import com.google.cloud.genomics.dataflow.pipelines.VerifyBamId.GetAlleleFreq;
import com.google.cloud.genomics.dataflow.pipelines.VerifyBamId.PileupAndJoinReads;
import com.google.cloud.genomics.dataflow.pipelines.VerifyBamId.SampleReads;
import com.google.cloud.genomics.dataflow.pipelines.VerifyBamId.SplitReads;
import com.google.common.collect.ImmutableList;
import com.google.genomics.v1.CigarUnit;
import com.google.genomics.v1.CigarUnit.Operation;
import com.google.genomics.v1.LinearAlignment;
import com.google.genomics.v1.Read;
import com.google.genomics.v1.Variant;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;

import com.beust.jcommander.internal.Lists;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the VerifyBamId pipeline.
 */
public class VerifyBamIdTest {
  
  @Test
  public void testSplitReads() {
    DoFnTester<Read, KV<Position, ReadBaseQuality>> splitReads = DoFnTester.of(new SplitReads());
    
    // single matched base -> one SingleReadQuality proto
    Read r = Read.newBuilder()
        .setProperPlacement(true)
        .setAlignment(LinearAlignment.newBuilder()
            .setPosition(com.google.genomics.v1.Position.newBuilder()
                .setReferenceName("1")
                .setPosition(123))
            .addCigar(CigarUnit.newBuilder()
                .setOperation(Operation.ALIGNMENT_MATCH)
                .setOperationLength(1)))
        .setAlignedSequence("A")
        .addAlignedQuality(3)
        .build();
    Assert.assertThat(splitReads.processBatch(r), CoreMatchers.hasItems(KV.of(new Position()
            .setReferenceName("1")
            .setPosition(123L),
            new ReadBaseQuality("A", 3))));
    
    // two matched bases -> two SingleReadQuality protos
    r = Read.newBuilder()
        .setProperPlacement(true)
        .setAlignment(LinearAlignment.newBuilder()
            .setPosition(com.google.genomics.v1.Position.newBuilder()
                .setReferenceName("1")
                .setPosition(123))
            .addCigar(CigarUnit.newBuilder()
                .setOperation(Operation.ALIGNMENT_MATCH)
                .setOperationLength(2)))
        .setAlignedSequence("AG")
        .addAllAlignedQuality(ImmutableList.of(3, 4))
        .build();
    Assert.assertThat(splitReads.processBatch(r), CoreMatchers.hasItems(KV.of(new Position()
            .setReferenceName("1")
            .setPosition(123L),
            new ReadBaseQuality("A", 3)),
        KV.of(new Position()
            .setReferenceName("1")
            .setPosition(124L),
            new ReadBaseQuality("G", 4))));
    
    // matched bases with different offsets onto the reference
    r = Read.newBuilder()
        .setProperPlacement(true)
        .setAlignment(LinearAlignment.newBuilder()
            .setPosition(com.google.genomics.v1.Position.newBuilder()
                .setReferenceName("1")
                .setPosition(123))
            .addCigar(CigarUnit.newBuilder()
                .setOperation(Operation.INSERT)
                .setOperationLength(1))
            .addCigar(CigarUnit.newBuilder()
                .setOperation(Operation.ALIGNMENT_MATCH)
                .setOperationLength(1))
            .addCigar(CigarUnit.newBuilder()
                .setOperation(Operation.DELETE)
                .setOperationLength(1))
            .addCigar(CigarUnit.newBuilder()
                .setOperation(Operation.ALIGNMENT_MATCH)
                .setOperationLength(2)))
         // 1I1M1D2M; C -> ref 0, GT -> ref 2, 3
        .setAlignedSequence("ACGT")
        .addAllAlignedQuality(ImmutableList.of(1, 2, 3, 4))
        .build();
    Assert.assertThat(splitReads.processBatch(r), CoreMatchers.hasItems(KV.of(new Position()
            .setReferenceName("1")
            .setPosition(123L),
            new ReadBaseQuality("C", 2)),
        KV.of(new Position()
            .setReferenceName("1")
            .setPosition(125L),
            new ReadBaseQuality("G", 3)),
        KV.of(new Position()
            .setReferenceName("1")
            .setPosition(126L),
            new ReadBaseQuality("T", 4))));
    
    // matched bases with different offsets onto the reference
    r = Read.newBuilder()
        .setAlignment(LinearAlignment.newBuilder()
            .setPosition(com.google.genomics.v1.Position.newBuilder()
                .setReferenceName("1")
                .setPosition(123))
            .addCigar(CigarUnit.newBuilder()
                .setOperation(Operation.DELETE)
                .setOperationLength(1))
            .addCigar(CigarUnit.newBuilder()
                .setOperation(Operation.ALIGNMENT_MATCH)
                .setOperationLength(2))
            .addCigar(CigarUnit.newBuilder()
                .setOperation(Operation.INSERT)
                .setOperationLength(1))
            .addCigar(CigarUnit.newBuilder()
                .setOperation(Operation.ALIGNMENT_MATCH)
                .setOperationLength(1)))
         // 1D2M1I1M; A and G match positions 1 and 3 of the ref
        .setAlignedSequence("ACGT")
        .addAllAlignedQuality(ImmutableList.of(1, 2, 3, 4))
        .build();
    Assert.assertThat(splitReads.processBatch(r), CoreMatchers.hasItems(KV.of(new Position()
            .setReferenceName("1")
            .setPosition(124L),
            new ReadBaseQuality("A", 1)),
        KV.of(new Position()
            .setReferenceName("1")
            .setPosition(125L),
            new ReadBaseQuality("C", 2)),
        KV.of(new Position()
            .setReferenceName("1")
            .setPosition(126L),
            new ReadBaseQuality("T", 4))));
  }
  
  @Test
  public void testSampleReads() {
    SampleReads sampleReads = new SampleReads(0.5, "");
    Assert.assertTrue(sampleReads.apply(KV.of(
        new Position()
            .setReferenceName("1")
            .setPosition(125L)
            .setReverseStrand(false),
        new ReadBaseQuality())));
    Assert.assertFalse(sampleReads.apply(KV.of(
        new Position()
            .setReferenceName("2")
            .setPosition(124L)
            .setReverseStrand(false),
        new ReadBaseQuality())));
  }
  
  @Test
  public void testGetAlleleFreq() {
    DoFnTester<Variant, KV<Position, AlleleFreq>> getAlleleFreq = DoFnTester.of(
        new GetAlleleFreq());
    Position pos = new Position().setReferenceName("1").setPosition(123L);
    Variant.Builder vBuild = Variant.newBuilder()
        .setReferenceName("1")
        .setStart(123L)
        .setReferenceBases("C")
        .addAlternateBases("T");
    vBuild.getMutableInfo().put("AF", ListValue.newBuilder()
        .addValues(Value.newBuilder().setNumberValue(0.25).build()).build());
    AlleleFreq af = new AlleleFreq();
    af.setAltBases(Lists.newArrayList("T"));
    af.setRefBases("C");
    af.setRefFreq(0.25);
    Assert.assertThat(getAlleleFreq.processBatch(vBuild.build()),
        CoreMatchers.hasItems(KV.of(pos, af)));
  }
  
  @Test
  public void testFilterFreq() {
    FilterFreq filterFreq = new FilterFreq(0.01);
    Position pos = new Position().setReferenceName("1").setPosition(123L);
    AlleleFreq af = new AlleleFreq();
    af.setRefFreq(0.9999);
    Assert.assertFalse(filterFreq.apply(KV.of(pos, af)));
    af.setRefFreq(0.9901);
    Assert.assertFalse(filterFreq.apply(KV.of(pos, af)));
    af.setRefFreq(0.9899);
    Assert.assertTrue(filterFreq.apply(KV.of(pos, af)));
  }
  
  private final Position position1
    = new Position()
      .setReferenceName("1")
      .setPosition(123L);
  private final Position position2
    = new Position()
      .setReferenceName("1")
      .setPosition(124L);
  private final Position position3
    = new Position()
      .setReferenceName("1")
      .setPosition(125L);

  private final ImmutableList<KV<Position, AlleleFreq>> refCountList;
  {
    ImmutableList.Builder<KV<Position, AlleleFreq>> refBuilder
      = ImmutableList.builder();
    AlleleFreq af = new AlleleFreq();
    af.setRefBases("A");
    af.setAltBases(Lists.newArrayList("G"));
    af.setRefFreq(0.8);
    refBuilder.add(KV.of(position1, af));
    af = new AlleleFreq();
    af.setRefBases("C");
    af.setAltBases(Lists.newArrayList("T"));
    af.setRefFreq(0.5);
    refBuilder.add(KV.of(position2, af));
    af = new AlleleFreq();
    af.setRefBases("T");
    af.setAltBases(Lists.newArrayList("C"));
    af.setRefFreq(0.6);
    refBuilder.add(KV.of(position3, af));
    refCountList = refBuilder.build();
  }
  
  @Test
  public void testPileupAndJoinReads() {
    VerifyBamId.VerifyBamIdOptions popts =
        PipelineOptionsFactory.create().as(VerifyBamId.VerifyBamIdOptions.class);
    Pipeline p = TestPipeline.create(popts);
    p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);
    ReadBaseQuality srq = new ReadBaseQuality("A", 10);
    PCollection<KV<Position, ReadBaseQuality>> readCounts = p.apply(
        Create.of(KV.of(position1, srq)));
    PCollection<KV<Position, AlleleFreq>> refFreq = p.apply(Create.of(refCountList));
    TupleTag<ReadBaseQuality> readCountsTag = new TupleTag<>();
    TupleTag<AlleleFreq> refFreqTag = new TupleTag<>();
    PCollection<KV<Position, CoGbkResult>> joined = KeyedPCollectionTuple
        .of(readCountsTag, readCounts)
        .and(refFreqTag, refFreq)
        .apply(CoGroupByKey.<Position>create());
    PCollection<KV<Position, ReadCounts>> result = joined.apply(
        ParDo.of(new PileupAndJoinReads(readCountsTag, refFreqTag)));
    ReadCounts rc = new ReadCounts();
    rc.setRefFreq(0.8);
    rc.addReadQualityCount(ReadQualityCount.Base.REF, 10, 1);
    DataflowAssert.that(result).containsInAnyOrder(KV.of(position1, rc));
  }
  
  @Test
  public void testCombineReads() {
    VerifyBamId.VerifyBamIdOptions popts =
        PipelineOptionsFactory.create().as(VerifyBamId.VerifyBamIdOptions.class);
    Pipeline p = TestPipeline.create(popts);
    p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);
    PCollection<KV<Position, AlleleFreq>> refCounts = p.apply(Create.of(this.refCountList));

    PCollection<Read> reads = p.apply(Create.of(Read.newBuilder()
        .setProperPlacement(true)
        .setAlignment(LinearAlignment.newBuilder()
            .setPosition(com.google.genomics.v1.Position.newBuilder()
                .setReferenceName("1")
                .setPosition(123))
            .addCigar(CigarUnit.newBuilder()
                .setOperation(Operation.ALIGNMENT_MATCH)
                .setOperationLength(3)))
        .setAlignedSequence("ATG")
        .addAllAlignedQuality(ImmutableList.of(3, 4, 5))
        .build()));
    
    PCollection<KV<Position, ReadCounts>> results =
        VerifyBamId.combineReads(reads, 1.0, "", refCounts);
    ReadCounts one = new ReadCounts();
    one.setRefFreq(0.8);
    one.addReadQualityCount(ReadQualityCount.Base.REF, 3, 1L);
    ReadCounts two = new ReadCounts();
    two.setRefFreq(0.5);
    two.addReadQualityCount(ReadQualityCount.Base.NONREF, 4, 1L);
    ReadCounts three = new ReadCounts();
    three.setRefFreq(0.6);
    three.addReadQualityCount(ReadQualityCount.Base.OTHER, 5, 1L);
    DataflowAssert.that(results)
        .containsInAnyOrder(KV.of(position1, one), KV.of(position2, two), KV.of(position3, three));
  }
}
