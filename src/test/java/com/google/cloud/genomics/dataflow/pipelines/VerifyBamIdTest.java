/*
 * Copyright 2015 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain ia copy of the License at
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

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.Pipeline;
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
import com.google.common.collect.Lists;
import com.google.genomics.v1.CigarUnit;
import com.google.genomics.v1.CigarUnit.Operation;
import com.google.genomics.v1.LinearAlignment;
import com.google.genomics.v1.Position;
import com.google.genomics.v1.Read;
import com.google.genomics.v1.Variant;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.logging.Logger;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import com.google.cloud.dataflow.sdk.transforms.DoFn;

/**
 * Tests for the VerifyBamId pipeline.
 */
@RunWith(JUnit4.class)
public class VerifyBamIdTest {
	private static final Logger LOG = Logger.getLogger(VerifyBamId.class.getName());	

  @Test
  public void testSplitReads_singleMatchedBase() {
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
    Assert.assertThat(splitReads.processBatch(r), CoreMatchers.hasItems(KV.of(Position.newBuilder()
            .setReferenceName("1")
            .setPosition(123L)
            .build(),
            new ReadBaseQuality("A", 3))));
  }

  @Test
  public void testSplitReads_twoMatchedBases() {
    DoFnTester<Read, KV<Position, ReadBaseQuality>> splitReads = DoFnTester.of(new SplitReads());

    // two matched bases -> two SingleReadQuality protos
    Read r = Read.newBuilder()
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
    Assert.assertThat(splitReads.processBatch(r), CoreMatchers.hasItems(KV.of(Position.newBuilder()
            .setReferenceName("1")
            .setPosition(123L)
            .build(),
            new ReadBaseQuality("A", 3)),
        KV.of(Position.newBuilder()
            .setReferenceName("1")
            .setPosition(124L)
            .build(),
            new ReadBaseQuality("G", 4))));
  }

  @Test
  public void testSplitReads_matchedBasesProperPlacementDifferentOffsets() {
    DoFnTester<Read, KV<Position, ReadBaseQuality>> splitReads = DoFnTester.of(new SplitReads());

    // matched bases with different offsets onto the reference
    Read r = Read.newBuilder()
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
    Assert.assertThat(splitReads.processBatch(r), CoreMatchers.hasItems(KV.of(Position.newBuilder()
            .setReferenceName("1")
            .setPosition(123L)
            .build(),
            new ReadBaseQuality("C", 2)),
        KV.of(Position.newBuilder()
            .setReferenceName("1")
            .setPosition(125L)
            .build(),
            new ReadBaseQuality("G", 3)),
        KV.of(Position.newBuilder()
            .setReferenceName("1")
            .setPosition(126L)
            .build(),
            new ReadBaseQuality("T", 4))));
  }

  @Test
  public void testSplitReads_twoMatchedBasesDifferentOffsets() {
    DoFnTester<Read, KV<Position, ReadBaseQuality>> splitReads = DoFnTester.of(new SplitReads());

    // matched bases with different offsets onto the reference
    Read r = Read.newBuilder()
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
    Assert.assertThat(splitReads.processBatch(r), CoreMatchers.hasItems(KV.of(Position.newBuilder()
            .setReferenceName("1")
            .setPosition(124L)
            .build(),
            new ReadBaseQuality("A", 1)),
        KV.of(Position.newBuilder()
            .setReferenceName("1")
            .setPosition(125L)
            .build(),
            new ReadBaseQuality("C", 2)),
        KV.of(Position.newBuilder()
            .setReferenceName("1")
            .setPosition(126L)
            .build(),
            new ReadBaseQuality("T", 4))));
  }

  @Test
  public void testSampleReads() {
    SampleReads sampleReads = new SampleReads(0.5, "");
    Assert.assertTrue(sampleReads.apply(KV.of(
        Position.newBuilder()
            .setReferenceName("1")
            .setPosition(125L)
            .setReverseStrand(false)
            .build(),
        new ReadBaseQuality())));
    Assert.assertFalse(sampleReads.apply(KV.of(
        Position.newBuilder()
            .setReferenceName("2")
            .setPosition(124L)
            .setReverseStrand(false)
            .build(),
        new ReadBaseQuality())));
  }

  @Test
  public void testGetAlleleFreq() {
    DoFnTester<Variant, KV<Position, AlleleFreq>> getAlleleFreq = DoFnTester.of(
        new GetAlleleFreq());
    Position pos = Position.newBuilder()
        .setReferenceName("1")
        .setPosition(123L)
        .build();
    Variant.Builder vBuild = Variant.newBuilder()
        .setReferenceName("1")
        .setStart(123L)
        .setReferenceBases("C")
        .addAlternateBases("T");
    vBuild.getMutableInfo().put("AF", ListValue.newBuilder()
        .addValues(Value.newBuilder().setStringValue("0.25").build()).build());
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
    Position pos = Position.newBuilder()
        .setReferenceName("1")
        .setPosition(123L)
        .build();
    AlleleFreq af = new AlleleFreq();
    af.setRefFreq(0.9999);
    Assert.assertFalse(filterFreq.apply(KV.of(pos, af)));
    af.setRefFreq(0.9901);
    Assert.assertFalse(filterFreq.apply(KV.of(pos, af)));
    af.setRefFreq(0.9899);
    Assert.assertTrue(filterFreq.apply(KV.of(pos, af)));
  }

  private final Position position1
    = Position.newBuilder()
      .setReferenceName("1")
      .setPosition(123L)
      .build();
  private final Position position2
    = Position.newBuilder()
      .setReferenceName("1")
      .setPosition(124L)
      .build();
  private final Position position3
    = Position.newBuilder()
      .setReferenceName("1")
      .setPosition(125L)
      .build();
  private final Position position1chrPrefix
  = Position.newBuilder()
    .setReferenceName("chr1")
    .setPosition(123L)
    .build();

	private final ReadCounts rc1;
	{		rc1 = new ReadCounts();
			rc1.setRefFreq(0.8);
			rc1.addReadQualityCount(ReadQualityCount.Base.REF, 10, 1);
	}

	private final ReadCounts rc2;
	{		rc2 = new ReadCounts();
			rc2.setRefFreq(0.5);
	}

	private final ReadCounts rc3;
	{		rc3 = new ReadCounts();
			rc3.setRefFreq(0.6);
	}

  private ImmutableList<KV<Position, AlleleFreq>> refCountList;
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
		VerifyBamId.Options popts =
        PipelineOptionsFactory.create().as(VerifyBamId.Options.class);
    Pipeline p = TestPipeline.create(popts);
    p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);

    final ReadBaseQuality srq = new ReadBaseQuality("A", 10);
    PCollection<KV<Position, ReadBaseQuality>> readCounts = p.apply(
        Create.of(KV.of(position1, srq)));
		DataflowAssert.that(readCounts).containsInAnyOrder(KV.of(position1, srq));

    PCollection<KV<Position, AlleleFreq>> refFreq = p.apply(Create.of(refCountList));

    DataflowAssert.that(refFreq).containsInAnyOrder(refCountList);

    final TupleTag<ReadBaseQuality> readCountsTag = new TupleTag<>();
    TupleTag<AlleleFreq> refFreqTag = new TupleTag<>();
    PCollection<KV<Position, CoGbkResult>> joined = KeyedPCollectionTuple
        .of(readCountsTag, readCounts)
        .and(refFreqTag, refFreq)
        .apply(CoGroupByKey.<Position>create());

    PCollection<KV<Position, ReadCounts>> result = joined.apply(
        ParDo.of(new PileupAndJoinReads(readCountsTag, refFreqTag)));
		
		KV<Position, ReadCounts> expectedResult1 = KV.of(position1, rc1);
		KV<Position, ReadCounts> expectedResult2 = KV.of(position2, rc2);
		KV<Position, ReadCounts> expectedResult3 = KV.of(position3, rc3);

    DataflowAssert.that(result).containsInAnyOrder(expectedResult1, expectedResult2, expectedResult3);
		p.run();
  }

  @Test
  public void testPileupAndJoinReadsWithChrPrefix() {
    VerifyBamId.Options popts =
        PipelineOptionsFactory.create().as(VerifyBamId.Options.class);
    Pipeline p = TestPipeline.create(popts);
    p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);

    ReadBaseQuality srq = new ReadBaseQuality("A", 10);
    PCollection<KV<Position, ReadBaseQuality>> readCounts = p.apply(
        Create.of(KV.of(position1chrPrefix, srq)));
    DataflowAssert.that(readCounts).containsInAnyOrder(KV.of(position1chrPrefix, srq));

    PCollection<KV<Position, AlleleFreq>> refFreq = p.apply(Create.of(refCountList));
    DataflowAssert.that(refFreq).containsInAnyOrder(refCountList);

    TupleTag<ReadBaseQuality> readCountsTag = new TupleTag<>();
    TupleTag<AlleleFreq> refFreqTag = new TupleTag<>();
    PCollection<KV<Position, CoGbkResult>> joined = KeyedPCollectionTuple
        .of(readCountsTag, readCounts)
        .and(refFreqTag, refFreq)
        .apply(CoGroupByKey.<Position>create());

    PCollection<KV<Position, ReadCounts>> result = joined.apply(
        ParDo.of(new PileupAndJoinReads(readCountsTag, refFreqTag)));
	
		KV<Position, ReadCounts> expectedResult1 = KV.of(position1, rc1);
		KV<Position, ReadCounts> expectedResult2 = KV.of(position2, rc2);
		KV<Position, ReadCounts> expectedResult3 = KV.of(position3, rc3);

    DataflowAssert.that(result).containsInAnyOrder(expectedResult1, expectedResult2, expectedResult3);
    p.run();
  }

  @Test
  public void testCombineReads() {
    VerifyBamId.Options popts =
        PipelineOptionsFactory.create().as(VerifyBamId.Options.class);
    Pipeline p = TestPipeline.create(popts);
    p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);

    PCollection<KV<Position, AlleleFreq>> refCounts = p.apply(Create.of(refCountList));
    DataflowAssert.that(refCounts).containsInAnyOrder(refCountList);

    Read read = Read.newBuilder()
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
        .build();

    PCollection<Read> reads = p.apply(Create.of(read));
    DataflowAssert.that(reads).containsInAnyOrder(read);

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
    p.run();
  }
}
