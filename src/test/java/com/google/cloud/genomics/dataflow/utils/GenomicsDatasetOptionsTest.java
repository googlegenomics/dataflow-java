/*
 * Copyright (C) 2014 Google Inc.
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
package com.google.cloud.genomics.dataflow.utils;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.genomics.dataflow.model.Contig;
import com.google.common.base.Joiner;

@RunWith(JUnit4.class)
public class GenomicsDatasetOptionsTest {

  @Test
  public void testGetContigs() {
    Contig brca1Contig = new Contig("17", 41196311, 41277499);
    String brca1ContigString = "17:41196311:41277499";

    Contig klothoContig = new Contig("13", 33628137, 33628138);
    String klothoContigString = "13:33628137:33628138";

    assertEquals(newArrayList(brca1Contig),
        newArrayList(GenomicsDatasetOptions.Methods.getContigs(brca1ContigString)));

    assertEquals(newArrayList(brca1Contig, klothoContig),
        newArrayList(GenomicsDatasetOptions.Methods.getContigs((Joiner.on(",").join(
            brca1ContigString, klothoContigString)))));
  }

}
