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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.utils.Contig;
import com.google.genomics.v1.StreamVariantsRequest;

@RunWith(JUnit4.class)
public class SitesToShardsTest {

  @Test
  public void testSiteParsing() {
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

    Assert.assertThat(sitesToContigsFn.processBatch(input),
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
  @Category(RunnableOnService.class)
  public void testSitesToStreamVariantsShards() throws Exception {

    final List<String> SITES  = Arrays.asList(new String[] {
        "some fake BED header",
        "chrX, 2000000, 3000000",
        "1, 2000000, 3000000",
        ""}); // blank line

    List<StreamVariantsRequest> expectedOutput = new ArrayList();
    expectedOutput.add(StreamVariantsRequest.newBuilder()
        .setVariantSetId("variantSetId")
        .setReferenceName("chrX")
        .setStart(2000000)
        .setEnd(3000000)
        .build());
    expectedOutput.add(StreamVariantsRequest.newBuilder()
        .setVariantSetId("variantSetId")
        .setReferenceName("1")
        .setStart(2000000)
        .setEnd(3000000)
        .build());

    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(SITES).withCoder(StringUtf8Coder.of()));

    PCollection<StreamVariantsRequest> output = input.apply("test transform",
        new SitesToShards.SitesToStreamVariantsShardsTransform("variantSetId"));

    DataflowAssert.that(output).containsInAnyOrder(expectedOutput);
    p.run();
  }

}
