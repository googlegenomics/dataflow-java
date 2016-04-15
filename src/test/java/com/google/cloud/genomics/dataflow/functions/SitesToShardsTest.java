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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.utils.Contig;
import com.google.genomics.v1.StreamVariantsRequest;

@RunWith(JUnit4.class)
public class SitesToShardsTest {

  @Test
  public void testSiteParsing() {
    SitesToShards.SitesToContigsFn fn = new SitesToShards.SitesToContigsFn();
    assertEquals(new Contig("chrX", 2000000, 3000000), fn.apply("chrX,2000000,3000000"));
    assertEquals(new Contig("chrX", 2000000, 3000000), fn.apply("chrX  2000000 3000000")); // tab
    assertEquals(new Contig("chrX", 2000000, 3000000), fn.apply("chrX 2000000 3000000")); // space
    assertEquals(new Contig("chrX", 2000000, 3000000), fn.apply("chrX:2000000:3000000"));

    // More white space
    assertEquals(new Contig("chrX", 2000000, 3000000), fn.apply(" chrX  2000000     3000000 "));
    // Additional fields
    assertEquals(new Contig("chrX", 2000000, 3000000), fn.apply("chrX,2000000,3000000,foo,bar"));

    // BED header
    assertNull(fn.apply("track name=pairedReads description=\"Clone Paired Reads\" useScore=1"));
  }

  // Test the PTransform by using an in-memory input and inspecting the output.
  @Test
  @Category(RunnableOnService.class)
  public void testSitesToStreamVariantsShards() throws Exception {

    final List<String> SITES  = Arrays.asList(new String[] {
        "chrX, 2000000, 3000000",
        "1, 2000000, 3000000"});

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
