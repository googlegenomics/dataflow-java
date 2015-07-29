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
package com.google.cloud.genomics.dataflow.pipelines;

import java.io.BufferedReader;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * This integration test will read and write to Cloud Storage, and call the Genomics API.
 *
 * The following environment variables are required:
 * - a Google Cloud API key in GOOGLE_API_KEY,
 * - a Google Cloud project name in TEST_PROJECT,
 * - a Cloud Storage folder path in TEST_OUTPUT_GCS_FOLDER to store temporary test outputs,
 * - a Cloud Storage folder path in TEST_STAGING_GCS_FOLDER to store temporary files,
 * 
 * Cloud Storage folder paths should be of the form "gs://bucket/folder/"
 *
 * When doing e.g. mvn install, you can skip integration tests using:
 *      mvn install -DskipITs
 *
 * To run one test:
 *      mvn -Dit.test=CountReadsITCase#testLocal verify
 *      
 * See also http://maven.apache.org/surefire/maven-failsafe-plugin/examples/single-test.html
 */
@RunWith(JUnit4.class)
public class CountReadsITCase {

  // This file shouldn't move.
  static final String TEST_BAM_FNAME = "gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/pilot_data/data/NA06985/alignment/NA06985.454.MOSAIK.SRP000033.2009_11.bam";
  // This is the Readgroupset ID of the same file, in ReadStore. It also shouldn't move.
  static final String TEST_READGROUPSET = "CMvnhpKTFhDvp9zAvYj66AY";
  // The region where we're counting reads.
  static final String TEST_CONTIG = "1:550000:560000";
  // How many reads are in that region.
  static final long TEST_EXPECTED = 685;
  // In this file there are no unmapped reads, so expecting the same number.
  static final long TEST_EXPECTED_WITH_UNMAPPED = TEST_EXPECTED;
  
  // Same as the above variables, but for the NA12877_S1 dataset.
  static final String NA12877_S1_BAM_FILENAME = "gs://genomics-public-data/platinum-genomes/bam/NA12877_S1.bam";
  static final String NA12877_S1_READGROUPSET = "CMvnhpKTFhD3he72j4KZuyc";
  static final String NA12877_S1_CONTIG = "chr17:41196311:41277499";
  static final long NA12877_S1_EXPECTED = 45081;
  // How many reads are in that region if we take unmapped ones too    
  static final long NA12877_S1_EXPECTED_WITH_UNMAPPED = 45142;

  static IntegrationTestHelper helper;
  
  @BeforeClass
  public static void setUpBeforeClass() {
    helper = new IntegrationTestHelper();
  }
  
  private void testLocalBase(String outputFilename, String contig, String bamFilename, long expectedCount,
      boolean includeUnmapped) throws Exception {
    final String OUTPUT = helper.TEST_OUTPUT_GCS_FOLDER + outputFilename;
    String[] ARGS = {
        "--apiKey=" + helper.API_KEY,
        "--output=" + OUTPUT,
        "--references=" + contig,
        "--includeUnmapped=" + includeUnmapped,
        "--BAMFilePath=" + bamFilename,
    };
    try {
      helper.touchOutput(OUTPUT);

      CountReads.main(ARGS);

      BufferedReader reader = helper.openOutput(OUTPUT);
      long got = Long.parseLong(reader.readLine());

    Assert.assertEquals(expectedCount, got);
    } finally {
      helper.deleteOutput(OUTPUT);
    }
  }

  /**
   * CountReads running on the client's machine.
   */
  @Test
  public void testLocal() throws Exception {
    testLocalBase("CountReadsITCase-testLocal-output.txt",
        TEST_CONTIG, TEST_BAM_FNAME, TEST_EXPECTED, false);
  }
  
  @Test
  public void testLocalUnmapped() throws Exception {
    testLocalBase("CountReadsITCase-testLocal-output.txt",
        TEST_CONTIG, TEST_BAM_FNAME, TEST_EXPECTED_WITH_UNMAPPED, true);
  }
  
  @Test
  public void testLocalNA12877_S1() throws Exception {
    testLocalBase("CountReadsITCase-testLocal-NA12877_S1-output.txt",
        NA12877_S1_CONTIG, NA12877_S1_BAM_FILENAME, NA12877_S1_EXPECTED, false);
  }
  
  @Test
  public void testLocalNA12877_S1_UNMAPPED() throws Exception {
    testLocalBase("CountReadsITCase-testLocal-NA12877_S1-output.txt",
        NA12877_S1_CONTIG, NA12877_S1_BAM_FILENAME, 
        NA12877_S1_EXPECTED_WITH_UNMAPPED, true);
  }

  private void testCloudBase(String outputFilename, String contig, String bamFilename, long expectedCount) throws Exception {
    final String OUTPUT = helper.TEST_OUTPUT_GCS_FOLDER + outputFilename;
    String[] ARGS = {
        "--apiKey=" + helper.API_KEY,
        "--project=" + helper.TEST_PROJECT,
        "--output=" + OUTPUT,
        "--numWorkers=2",
        "--runner=BlockingDataflowPipelineRunner",
        "--stagingLocation=" + helper.TEST_STAGING_GCS_FOLDER,
        "--references=" + contig,
        "--BAMFilePath=" + bamFilename
    };
    try {
      helper.touchOutput(OUTPUT);

      CountReads.main(ARGS);

      BufferedReader reader = helper.openOutput(OUTPUT);
      long got = Long.parseLong(reader.readLine());

      Assert.assertEquals(expectedCount, got);
    } finally {
      helper.deleteOutput(OUTPUT);
    }
  }
  
  /**
   * CountReads running on Dataflow.
   */
  @Test
  public void testCloud() throws Exception {
    testCloudBase("CountReadsITCase-testCloud-output.txt",
        TEST_CONTIG, TEST_BAM_FNAME, TEST_EXPECTED);
  }
  
  @Test
  public void testCloudNA12877_S1() throws Exception {
    testCloudBase("CountReadsITCase-testCloud-NA12877_S1-output.txt",
        NA12877_S1_CONTIG, NA12877_S1_BAM_FILENAME, NA12877_S1_EXPECTED);
  }

  public void testCloudWithAPIBase(String outputFilename, String contig, String readGroupSetId, long expectedCount) throws Exception {
    final String OUTPUT = helper.TEST_OUTPUT_GCS_FOLDER + outputFilename;
    String[] ARGS = {
        "--apiKey=" + helper.API_KEY,
        "--project=" + helper.TEST_PROJECT,
        "--output=" + OUTPUT,
        "--numWorkers=2",
        "--runner=BlockingDataflowPipelineRunner",
        "--stagingLocation=" + helper.TEST_STAGING_GCS_FOLDER,
        "--references=" + contig,
        "--readGroupSetId=" + readGroupSetId
    };
    try {
      helper.touchOutput(OUTPUT);

      CountReads.main(ARGS);

      BufferedReader reader = helper.openOutput(OUTPUT);
      long got = Long.parseLong(reader.readLine());

      Assert.assertEquals(expectedCount, got);
    } finally {
      helper.deleteOutput(OUTPUT);
    }
  }

  /**
   * CountReads running on Dataflow with API input.
   */
  @Test
  public void testCloudWithAPI() throws Exception {
    testCloudWithAPIBase("CountReadsITCase-testCloudWithAPI-output.txt",
        TEST_CONTIG, TEST_READGROUPSET, TEST_EXPECTED);
  }

  @Test
  public void testCloudWithAPI_NA12877_S1() throws Exception {
    testCloudWithAPIBase("CountReadsITCase-testCloudWithAPI-NA12877_S1-output.txt",
        NA12877_S1_CONTIG, NA12877_S1_READGROUPSET, NA12877_S1_EXPECTED);
  }



}