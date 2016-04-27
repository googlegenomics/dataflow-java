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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import htsjdk.samtools.BAMIndexMetaData;
import htsjdk.samtools.SamReader;

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
 *      mvn -Dit.test=ShardedBAMWritingITCase#testShardedWriting verify
 *
 * See also http://maven.apache.org/surefire/maven-failsafe-plugin/examples/single-test.html
 */
@RunWith(JUnit4.class)
public class ShardedBAMWritingITCase {
  static final String TEST_CONTIG = "11:1:200000000";
  static final String TEST_BAM_FNAME = "gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/phase3/data/NA12878/exome_alignment/NA12878.chrom11.ILLUMINA.bwa.CEU.exome.20121211.bam";
  static final String OUTPUT_FNAME = "sharded-output.bam";
  static final int EXPECTED_ALL_READS = 10414236;
  static final int EXPECTED_UNMAPPED_READS = 108950;


  static IntegrationTestHelper helper;

  @BeforeClass
  public static void setUpBeforeClass() {
    helper = new IntegrationTestHelper();
  }

  @Test
  public void testShardedWriting() throws Exception {
    final String OUTPUT = helper.getTestOutputGcsFolder() + OUTPUT_FNAME;
    String[] ARGS = {
        "--project=" + helper.getTestProject(),
        "--output=" + OUTPUT,
        "--numWorkers=18",
        "--runner=BlockingDataflowPipelineRunner",
        "--stagingLocation=" + helper.getTestStagingGcsFolder(),
        "--references=" + TEST_CONTIG,
        "--BAMFilePath=" + TEST_BAM_FNAME,
        "--lociPerWritingShard=1000000"
    };
    SamReader reader = null;
    try {
      helper.touchOutput(OUTPUT);

      ShardedBAMWriting.main(ARGS);

      reader = helper.openBAM(OUTPUT);
      Assert.assertTrue(reader.hasIndex());
      final int sequenceIndex = reader.getFileHeader().getSequenceIndex("11");
      BAMIndexMetaData metaData = reader.indexing().getIndex().getMetaData(sequenceIndex);
      Assert.assertEquals(EXPECTED_ALL_READS - EXPECTED_UNMAPPED_READS,
          metaData.getAlignedRecordCount());
      // Not handling unmapped reads yet
      // Assert.assertEquals(EXPECTED_UNMAPPED_READS,
      // metaData.getUnalignedRecordCount());

    } finally {
      if (reader != null) {
        reader.close();
      }
      helper.deleteOutput(OUTPUT);
    }
  }
}