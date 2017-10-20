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

import com.google.api.services.genomics.model.Annotation;
import com.google.api.services.genomics.model.Position;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.model.PosRgsMq;
import com.google.cloud.genomics.dataflow.pipelines.CalculateCoverage.CalculateCoverageMean;
import com.google.cloud.genomics.dataflow.pipelines.CalculateCoverage.CalculateQuantiles;
import com.google.common.collect.Lists;
import com.google.genomics.v1.CigarUnit;
import com.google.genomics.v1.CigarUnit.Operation;
import com.google.genomics.v1.LinearAlignment;
import com.google.genomics.v1.Read;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Class for testing the Calculate Coverage class
 */
@RunWith(JUnit4.class)
public class CalculateCoverageTest {

  private static List<Read> testSet;
  private static List<KV<PosRgsMq, Double>> testSet2;
  // The bucket width we are calculating coverage for.
  static final int TEST_BUCKET_WIDTH = 2;
  // The number of quantiles are are computing.
  static final int TEST_NUM_QUANTILES = 3;
  // Test pipeline options.
  static CalculateCoverage.Options popts;
  // The input data, see setup
  static List<Read> input;
  // Input read position info
  static final long[] readPosInfo = {0, 1, 2, 0, 0, 0, 0, 2, 3, 0, 0, 0, 3, 2, 0,
    0, 0, 0, 1, 3, 0, 0, 1, 0, 2};
  // Input read mapping quality info, (L = 5, M = 15, H = 35)
  static final int[] readMQInfo = {5, 15, 35, 15, 35, 5, 5, 15, 15, 35, 5, 15, 35, 35, 15,
    5, 15, 15, 35, 35, 5, 5, 5, 5, 5};
  // Input read length info
  static final int[] readLengthInfo = {4, 3, 1, 3, 1, 4, 3, 2, 1, 1, 4, 2, 1, 1, 3,
    4, 2, 1, 2, 1, 4, 3, 3, 2, 1};

  static final Operation[] ops1 = {Operation.INSERT, Operation.SEQUENCE_MISMATCH, Operation.PAD};
  static final Operation[] ops2 = {Operation.DELETE, Operation.SKIP, Operation.SEQUENCE_MATCH};
  static final Operation[] ops3 =
      {Operation.INSERT, Operation.ALIGNMENT_MATCH, Operation.CLIP_SOFT};

  @Rule
  public final transient TestPipeline p = TestPipeline.fromOptions(popts);

  @BeforeClass
  public static void oneTimeSetUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);

    popts = PipelineOptionsFactory.create().as(
      CalculateCoverage.Options.class);
    popts.setBucketWidth(TEST_BUCKET_WIDTH);
    popts.setNumQuantiles(TEST_NUM_QUANTILES);

    // Test data for testCalculateCoverageMean
    // Read 1 (Contains two operations that result in increasing the length of the read)
    List<CigarUnit> cigars = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      CigarUnit u = CigarUnit.newBuilder().setOperationLength(2L).setOperation(ops1[i]).build();
      CigarUnit.newBuilder().setOperation(CigarUnit.Operation.SKIP);
      cigars.add(u);
    }
    Read read2ValidOps = Read.newBuilder()
        .setAlignment(LinearAlignment.newBuilder()
            .addAllCigar(cigars)
            .setPosition(com.google.genomics.v1.Position.newBuilder()
                .setPosition(3L)
                .setReferenceName("chr1"))
            .setMappingQuality(15))
        .setReadGroupSetId("123").build();
    // Read 2 (Contains three operations that result in increasing the length of the read)
    cigars.clear();
    for (int i = 0; i < 3; i++) {
      CigarUnit u = CigarUnit.newBuilder().setOperationLength(1L).setOperation(ops2[i]).build();
      cigars.add(u);
    }
    Read read3ValidOps = Read.newBuilder()
        .setAlignment(LinearAlignment.newBuilder()
            .addAllCigar(cigars)
            .setPosition(com.google.genomics.v1.Position.newBuilder()
                .setPosition(2L)
                .setReferenceName("chr1"))
            .setMappingQuality(1))
        .setReadGroupSetId("123").build();
    // Read 3 (Unmapped)
    Read unmappedRead = Read.newBuilder().build();
    // Read 4 (Contains one operation that results in increasing the length of the read)
    cigars.clear();
    for (int i = 0; i < 3; i++) {
      CigarUnit u = CigarUnit.newBuilder().setOperationLength(4L).setOperation(ops3[i]).build();
      cigars.add(u);
    }
    Read read1ValidOp = Read.newBuilder()
        .setAlignment(LinearAlignment.newBuilder()
            .addAllCigar(cigars)
            .setPosition(com.google.genomics.v1.Position.newBuilder()
                .setPosition(4L)
                .setReferenceName("chr1"))
            .setMappingQuality(1))
        .setReadGroupSetId("321").build();
    testSet = Lists.newArrayList(read2ValidOps, read3ValidOps, unmappedRead, read1ValidOp);
    // Test data for testCalculateQuantiles
    testSet2 = Lists.newArrayList();
    Position p = new Position().setPosition(1L).setReferenceName("chr1");
    testSet2.add(KV.of(new PosRgsMq(p, "123", PosRgsMq.MappingQuality.L), 4.0));
    testSet2.add(KV.of(new PosRgsMq(p, "123", PosRgsMq.MappingQuality.M), 2.0));
    testSet2.add(KV.of(new PosRgsMq(p, "123", PosRgsMq.MappingQuality.H), 0.5));
    testSet2.add(KV.of(new PosRgsMq(p, "123", PosRgsMq.MappingQuality.A), 6.5));
    testSet2.add(KV.of(new PosRgsMq(p, "321", PosRgsMq.MappingQuality.L), 5.0));
    testSet2.add(KV.of(new PosRgsMq(p, "321", PosRgsMq.MappingQuality.M), 1.0));
    testSet2.add(KV.of(new PosRgsMq(p, "321", PosRgsMq.MappingQuality.H), 0.75));
    testSet2.add(KV.of(new PosRgsMq(p, "321", PosRgsMq.MappingQuality.A), 6.75));
    testSet2.add(KV.of(new PosRgsMq(p, "456", PosRgsMq.MappingQuality.L), 4.6));
    testSet2.add(KV.of(new PosRgsMq(p, "456", PosRgsMq.MappingQuality.M), 3.2));
    testSet2.add(KV.of(new PosRgsMq(p, "456", PosRgsMq.MappingQuality.H), 1.2));
    testSet2.add(KV.of(new PosRgsMq(p, "456", PosRgsMq.MappingQuality.A), 9.0));
    testSet2.add(KV.of(new PosRgsMq(p, "654", PosRgsMq.MappingQuality.L), 3.0));
    testSet2.add(KV.of(new PosRgsMq(p, "654", PosRgsMq.MappingQuality.A), 3.0));
    testSet2.add(KV.of(new PosRgsMq(p, "789", PosRgsMq.MappingQuality.L), 8.0));
    testSet2.add(KV.of(new PosRgsMq(p, "789", PosRgsMq.MappingQuality.A), 8.0));
    // Test data for testCalculateCoverage
    input = Lists.newArrayList();
    cigars.clear();
    for (int i = 0; i < 25; i++) {
      cigars = new ArrayList<>();
      for (int j = 0; j < readLengthInfo[i]; j++) {
        CigarUnit u = CigarUnit.newBuilder().setOperationLength(1L).setOperation(Operation.ALIGNMENT_MATCH).build();
        cigars.add(u);
      }
      Read read = Read.newBuilder()
          .setAlignment(LinearAlignment.newBuilder()
              .addAllCigar(cigars)
              .setPosition(com.google.genomics.v1.Position.newBuilder()
                  .setPosition(readPosInfo[i])
                  .setReferenceName("1"))
              .setMappingQuality(readMQInfo[i]))
          .setReadGroupSetId("Rgs" + i / 5 + 1).build();
      input.add(read);
    }
  }

  @Before
  public void setUp() {
    CalculateCoverage.registerPipelineCoders(p);
  }

  /**
   * Unit test for CalculateCoverageMean composite PTransform
   */
  @Test
  public void testCalculateCoverageMean() {
    // Expected Output
    List<KV<PosRgsMq, Double>> expectedOutput = new ArrayList<KV<PosRgsMq, Double>>();

    PosRgsMq pTest = new PosRgsMq(new Position()
        .setPosition(2L).setReferenceName("chr1"), "123", PosRgsMq.MappingQuality.L);
    expectedOutput.add(KV.of(pTest, 1.0));
    pTest = new PosRgsMq(new Position()
        .setPosition(2L).setReferenceName("chr1"), "123", PosRgsMq.MappingQuality.M);
    expectedOutput.add(KV.of(pTest, 0.5));
    pTest = new PosRgsMq(new Position()
        .setPosition(2L).setReferenceName("chr1"), "123", PosRgsMq.MappingQuality.A);
    expectedOutput.add(KV.of(pTest, 1.5));
    pTest = new PosRgsMq(new Position()
        .setPosition(4L).setReferenceName("chr1"), "123", PosRgsMq.MappingQuality.L);
    expectedOutput.add(KV.of(pTest, 0.5));
    pTest = new PosRgsMq(new Position()
        .setPosition(4L).setReferenceName("chr1"), "123", PosRgsMq.MappingQuality.M);
    expectedOutput.add(KV.of(pTest, 1.0));
    pTest = new PosRgsMq(new Position()
        .setPosition(4L).setReferenceName("chr1"), "123", PosRgsMq.MappingQuality.A);
    expectedOutput.add(KV.of(pTest, 1.5));
    pTest = new PosRgsMq(new Position()
        .setPosition(6L).setReferenceName("chr1"), "123", PosRgsMq.MappingQuality.M);
    expectedOutput.add(KV.of(pTest, 0.5));
    pTest = new PosRgsMq(new Position()
        .setPosition(6L).setReferenceName("chr1"), "123", PosRgsMq.MappingQuality.A);
    expectedOutput.add(KV.of(pTest, 0.5));
    pTest = new PosRgsMq(new Position()
        .setPosition(4L).setReferenceName("chr1"), "321", PosRgsMq.MappingQuality.L);
    expectedOutput.add(KV.of(pTest, 1.0));
    pTest = new PosRgsMq(new Position()
        .setPosition(4L).setReferenceName("chr1"), "321", PosRgsMq.MappingQuality.A);
    expectedOutput.add(KV.of(pTest, 1.0));
    pTest = new PosRgsMq(new Position()
        .setPosition(6L).setReferenceName("chr1"), "321", PosRgsMq.MappingQuality.L);
    expectedOutput.add(KV.of(pTest, 1.0));
    pTest = new PosRgsMq(new Position()
        .setPosition(6L).setReferenceName("chr1"), "321", PosRgsMq.MappingQuality.A);
    expectedOutput.add(KV.of(pTest, 1.0));

    PCollection<Read> inputReads = p.apply(Create.of(testSet));
    PCollection<KV<PosRgsMq, Double>> output = inputReads.apply(
        new CalculateCoverageMean(popts.getBucketWidth()));
    PAssert.that(output).containsInAnyOrder(expectedOutput);
    p.run();
  }

  /**
   * Unit test for CalculateQuantiles composite PTransform
   */
  @Ignore // TODO: this needs a coder for com.google.api.client.util.DataMap$Entry which is private.
  @Test
  public void testCalculateQuantiles() {
    PCollection<KV<PosRgsMq, Double>> inputMappingQualities = p.apply(Create.of(testSet2));
    PCollection<KV<Position, KV<PosRgsMq.MappingQuality, List<Double>>>> output = inputMappingQualities.apply(
        new CalculateQuantiles(3));
    Position pos = new Position().setPosition(1L).setReferenceName("chr1");
    List<Double> low = Lists.newArrayList(3.0, 4.6, 8.0);
    List<Double> med = Lists.newArrayList(1.0, 2.0, 3.2);
    List<Double> high = Lists.newArrayList(0.5, 0.75, 1.2);
    List<Double> all = Lists.newArrayList(3.0, 6.75, 9.0);
    PAssert.that(output).containsInAnyOrder(
        KV.of(pos, KV.of(PosRgsMq.MappingQuality.L, low)),
        KV.of(pos, KV.of(PosRgsMq.MappingQuality.M, med)),
        KV.of(pos, KV.of(PosRgsMq.MappingQuality.H, high)),
        KV.of(pos, KV.of(PosRgsMq.MappingQuality.A, all)));
    p.run();
  }

  /**
   * Testing the CalculateCoverage pipeline.
   */
  @Test
  public void testCalculateCoverage() throws Exception {
    List<Annotation> expectedOutput = Lists.newArrayList();
    Annotation a1 = new Annotation()
        .setAnnotationSetId("123")
        .setStart(0L)
        .setEnd(2L)
        .setReferenceName("1")
        .setType("GENERIC")
        .setInfo(new HashMap<String, List<Object>>());
    a1.getInfo().put("L", Lists.newArrayList((Object) "1.0", "1.0", "3.5"));
    a1.getInfo().put("M", Lists.newArrayList((Object) "1.5", "1.5", "2.0"));
    a1.getInfo().put("H", Lists.newArrayList((Object) "0.5", "0.5", "0.5"));
    a1.getInfo().put("A", Lists.newArrayList((Object) "2.5", "3.0", "3.5"));
    expectedOutput.add(a1);
    Annotation a2 = new Annotation()
        .setAnnotationSetId("123")
        .setStart(2L)
        .setEnd(4L)
        .setReferenceName("1")
        .setType("GENERIC")
        .setInfo(new HashMap<String, List<Object>>());
    a2.getInfo().put("L", Lists.newArrayList((Object) "1.0", "1.0", "3.0"));
    a2.getInfo().put("M", Lists.newArrayList((Object) "0.5", "1.5", "1.5"));
    a2.getInfo().put("H", Lists.newArrayList((Object) "0.5", "1.0", "1.0"));
    a2.getInfo().put("A", Lists.newArrayList((Object) "2.0", "3.0", "3.0"));
    expectedOutput.add(a2);

    PCollection<Read> reads = p.apply(Create.of(input));
    PCollection<KV<PosRgsMq, Double>> coverageMeans = reads.apply(
        new CalculateCoverage.CalculateCoverageMean(popts.getBucketWidth()));

    PCollection<KV<Position, KV<PosRgsMq.MappingQuality, List<Double>>>> quantiles
        = coverageMeans.apply(new CalculateCoverage.CalculateQuantiles(popts.getNumQuantiles()));

    PCollection<KV<Position, Iterable<KV<PosRgsMq.MappingQuality, List<Double>>>>> answer
        = quantiles.apply(GroupByKey.<Position, KV<PosRgsMq.MappingQuality, List<Double>>>create());

    PCollection<Annotation> output = answer.apply(
        ParDo.of(new CalculateCoverage.CreateAnnotations("123", null, false)));

    PAssert.that(output).containsInAnyOrder(expectedOutput);

    p.run();
  }
}
