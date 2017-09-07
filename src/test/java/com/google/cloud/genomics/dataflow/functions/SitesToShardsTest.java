/*
 * Copyright (C) 2016 Google Inc.
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
package com.google.cloud.genomics.dataflow.functions;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.PCollection;
import com.google.cloud.genomics.utils.Contig;
import com.google.genomics.v1.StreamVariantsRequest;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(JUnit4.class)
public class SitesToShardsTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  @Test
  public void testSiteParsing() throws Exception {
    DoFnTester<String, Contig> sitesToContigsFn =
        DoFnTester.of(new SitesToShards.SitesToContigsFn());
    String [] input = {
        "chrX,2000001,3000000",
        "chrX  2000002 3000000", // tab
        "chrX 2000003 3000000", // space
        "chrX:2000004:3000000", // colon
        " chrX  2000005     3000000 ", // more white space
        "chrX,2000006,3000000,foo,bar", // additional fields
        "17,2000000,3000000",
        "M,2000000,3000000",
        "chr9_gl000199_random,2000000,3000000",
        "GL.123,2000000,3000000",
        "track name=pairedReads description=\"Clone Paired Reads\" useScore=1", // BED header
    };

    Assert.assertThat(sitesToContigsFn.processBundle(input),
        IsIterableContainingInAnyOrder.containsInAnyOrder(
            new Contig("chrX", 2000001, 3000000),
            new Contig("chrX", 2000002, 3000000),
            new Contig("chrX", 2000003, 3000000),
            new Contig("chrX", 2000004, 3000000),
            new Contig("chrX", 2000005, 3000000),
            new Contig("chrX", 2000006, 3000000),
            new Contig("17", 2000000, 3000000),
            new Contig("M", 2000000, 3000000),
            new Contig("chr9_gl000199_random", 2000000, 3000000),
            new Contig("GL.123", 2000000, 3000000)
            ));
  }

  // Test the PTransform by using an in-memory input and inspecting the output.
  @Test
  public void testSitesToStreamVariantsShards() throws Exception {

    final List<String> SITES  = Arrays.asList(new String[] {
        "some fake BED header",
        "chrX, 2000000, 3000000",
        "1, 2000000, 3000000",
        ""}); // blank line

    StreamVariantsRequest prototype = StreamVariantsRequest.newBuilder()
        .setProjectId("theProjectId")
        .setVariantSetId("theVariantSetId")
        .build();

    List<StreamVariantsRequest> expectedOutput = new ArrayList();
    expectedOutput.add(StreamVariantsRequest.newBuilder(prototype)
        .setReferenceName("chrX")
        .setStart(2000000)
        .setEnd(3000000)
        .build());
    expectedOutput.add(StreamVariantsRequest.newBuilder(prototype)
        .setReferenceName("1")
        .setStart(2000000)
        .setEnd(3000000)
        .build());

    PCollection<String> input = p.apply(Create.of(SITES).withCoder(StringUtf8Coder.of()));

    PCollection<StreamVariantsRequest> output = input.apply("test transform",
        new SitesToShards.SitesToStreamVariantsShardsTransform(prototype));

    PAssert.that(output).containsInAnyOrder(expectedOutput);
    p.run();
  }

}
