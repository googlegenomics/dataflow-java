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
package com.google.cloud.genomics.dataflow.pipelines;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Lists;
import com.google.api.services.storage.Storage;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.genomics.dataflow.readers.bam.BAMIO;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;

import htsjdk.samtools.SamReader;
import htsjdk.samtools.ValidationStringency;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.List;

public class IntegrationTestHelper {

  // Test configuration constants
  private final String TEST_PROJECT = System.getenv("TEST_PROJECT");
  private final String TEST_OUTPUT_GCS_FOLDER = System.getenv("TEST_OUTPUT_GCS_FOLDER");
  private final String TEST_STAGING_GCS_FOLDER = System.getenv("TEST_STAGING_GCS_FOLDER");

  // Variant test configuration constants
  public static final String PLATINUM_GENOMES_DATASET = "3049512673186936334";
  public static final String PLATINUM_GENOMES_BRCA1_REFERENCES = "chr17:41196311:41277499";
  public static final int PLATINUM_GENOMES_NUMBER_OF_SAMPLES = 17;
  public static final String A_FEW_PLATINUM_GENOMES_CALLSET_NAMES = "NA12877,NA12889,NA12890";

  private GenomicsOptions popts = PipelineOptionsFactory.create().as(GenomicsOptions.class);
  GcsUtil gcsUtil;

  public IntegrationTestHelper() {
    assertNotNull("You must set the TEST_PROJECT environment variable for this test.", TEST_PROJECT);
    assertNotNull("You must set the TEST_OUTPUT_GCS_FOLDER environment variable for this test.", TEST_OUTPUT_GCS_FOLDER);
    assertNotNull("You must set the TEST_STAGING_GCS_FOLDER environment variable for this test.", TEST_STAGING_GCS_FOLDER);
    assertTrue("TEST_OUTPUT_GCS_FOLDER must end with '/'", TEST_OUTPUT_GCS_FOLDER.endsWith("/"));
    assertTrue("TEST_OUTPUT_GCS_FOLDER must start with 'gs://'", TEST_OUTPUT_GCS_FOLDER.startsWith("gs://"));
    assertTrue("TEST_STAGING_GCS_FOLDER must start with 'gs://'", TEST_STAGING_GCS_FOLDER.startsWith("gs://"));
    // we don't care how TEST_STAGING_GCS_FOLDER ends, so no check for it.

    gcsUtil = new GcsUtil.GcsUtilFactory().create(popts);
  }

  /**
   * @return the TEST_PROJECT
   */
  public String getTestProject() {
    return TEST_PROJECT;
  }

  /**
   * @return the TEST_OUTPUT_GCS_FOLDER
   */
  public String getTestOutputGcsFolder() {
    return TEST_OUTPUT_GCS_FOLDER;
  }

  /**
   * @return the TEST_STAGING_GCS_FOLDER
   */
  public String getTestStagingGcsFolder() {
    return TEST_STAGING_GCS_FOLDER;
  }

  /**
   * Make sure we can get to the output for single-file test results.
   *
   * Also write a sentinel value to the file.  This protects against the possibility of prior
   * test output causing a newly failing test to appear to succeed.
   *
   * @param outputPath
   * @throws IOException
   */
  public void touchOutput(String outputPath) throws IOException {
    try (Writer writer = Channels.newWriter(gcsUtil.create(GcsPath.fromUri(outputPath), "text/plain"), "UTF-8")) {
      writer.write("output will go here");
    }
  }

  /**
   * Open test output for reading for single file test results.
   */
  public BufferedReader openOutput(String outputPath) throws IOException {
    return new BufferedReader(Channels.newReader(gcsUtil.open(GcsPath.fromUri(outputPath)), "UTF-8"));
  }

  /**
   * Open test output as BAM file - useful if your test writes out a BAM file
   * and you want to validate the contents.
   * @throws IOException
   */
  public SamReader openBAM(String bamFilePath) throws IOException {
    final GcsOptions gcsOptions = popts.as(GcsOptions.class);
    final Storage.Objects storage = Transport.newStorageClient(gcsOptions).build().objects();
    return BAMIO.openBAM(storage, bamFilePath, ValidationStringency.LENIENT, true);
  }

  /**
   * Download multi file test output.
   *
   * Some cloud storage operations are eventually consistent
   * https://cloud.google.com/storage/docs/consistency so
   * be robust for those cases.
   *
   * @param outputPrefix prefix for files to download
   * @param numLinesExpected number of line expected
   * @return lines from all test output files
   * @throws Exception
   */
  public List<String> downloadOutputs(String outputPrefix, int numLinesExpected) throws Exception {
    // Download the pipeline results.
    List<String> results = null;

    ExponentialBackOff backoff = new ExponentialBackOff.Builder()
      .setMaxIntervalMillis(6000)
      .build();

    while (true) {
      try {
        results = Lists.newArrayList();
        for (GcsPath path : gcsUtil.expand(GcsPath.fromUri(outputPrefix + "*"))) {
          BufferedReader reader = openOutput(path.toString());
          for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            results.add(line);
          }
          reader.close();
        }
      } catch (FileNotFoundException e) {
        long backOffMillis = backoff.nextBackOffMillis();
        if (backOffMillis == BackOff.STOP) {
          throw e;
        }
        Thread.sleep(backOffMillis);
      }
      long backOffMillis = backoff.nextBackOffMillis();
      if (numLinesExpected == results.size()
          || backOffMillis == BackOff.STOP) {
        // If we have the number of results we expect OR we've used all the retries
        // (e.g., due to a real test failure), exit from this loop.
        break;
      }
      Thread.sleep(backOffMillis);
    }
    return results;
  }

  /**
   * Delete single file test output.
   *
   * @param outputPath path to the output file to be deleted.
   * @throws Exception
   */
  public void deleteOutput(String outputPath) throws Exception {
    // boilerplate
    GcsPath path = GcsPath.fromUri(outputPath);
    GcsOptions gcsOptions = popts.as(GcsOptions.class);
    Storage storage = Transport.newStorageClient(gcsOptions).build();
    // do the actual work
    storage.objects().delete(path.getBucket(), path.getObject()).execute();
  }

  /**
   * Delete multi file test output.
   *
   * @param outputPrefix prefix for files to delete
   * @throws Exception
   */
  public void deleteOutputs(String outputPrefix) throws Exception {
    for (GcsPath path : gcsUtil.expand(GcsPath.fromUri(outputPrefix + "*"))) {
      deleteOutput(path.toString());
    }
  }

}
