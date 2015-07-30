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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.security.GeneralSecurityException;

import org.junit.Assert;

import com.google.api.services.storage.Storage;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;

public class IntegrationTestHelper {

  // Test configuration constants
  final String API_KEY = System.getenv("GOOGLE_API_KEY");
  final String TEST_PROJECT = System.getenv("TEST_PROJECT");
  final String TEST_OUTPUT_GCS_FOLDER = System.getenv("TEST_OUTPUT_GCS_FOLDER");
  final String TEST_STAGING_GCS_FOLDER = System.getenv("TEST_STAGING_GCS_FOLDER");

  // Variant test configuration constants
  static final String PLATINUM_GENOMES_DATASET = "3049512673186936334";
  static final String PLATINUM_GENOMES_BRCA1_REFERENCES = "chr17:41196311:41277499";
  static final int PLATINUM_GENOMES_NUMBER_OF_SAMPLES = 17;

  GenomicsOptions popts = PipelineOptionsFactory.create().as(GenomicsOptions.class);
  GcsUtil gcsUtil;
  
  public IntegrationTestHelper() {
    Assert.assertNotNull("You must set the GOOGLE_API_KEY environment variable for this test.", API_KEY);
    Assert.assertNotNull("You must set the TEST_PROJECT environment variable for this test.", TEST_PROJECT);
    Assert.assertNotNull("You must set the TEST_OUTPUT_GCS_FOLDER environment variable for this test.", TEST_OUTPUT_GCS_FOLDER);
    Assert.assertNotNull("You must set the TEST_STAGING_GCS_FOLDER environment variable for this test.", TEST_STAGING_GCS_FOLDER);
    Assert.assertTrue("TEST_OUTPUT_GCS_FOLDER must end with '/'", TEST_OUTPUT_GCS_FOLDER.endsWith("/"));
    Assert.assertTrue("TEST_OUTPUT_GCS_FOLDER must start with 'gs://'", TEST_OUTPUT_GCS_FOLDER.startsWith("gs://"));
    Assert.assertTrue("TEST_STAGING_GCS_FOLDER must start with 'gs://'", TEST_STAGING_GCS_FOLDER.startsWith("gs://"));
    // we don't care how TEST_STAGING_GCS_FOLDER ends, so no check for it.
    
    popts.setApiKey(API_KEY);
    gcsUtil = new GcsUtil.GcsUtilFactory().create(popts);
  }

  /**
   * Make sure we can get to the output.
   * 
   * Also write a sentinel value to the file.  This protects against the possibility of prior
   * test output causing a newly failing test to appear to succeed.
   */
  public void touchOutput(String outputPath) throws IOException {
    try (Writer writer = Channels.newWriter(gcsUtil.create(GcsPath.fromUri(outputPath), "text/plain"), "UTF-8")) {
      writer.write("output will go here");
    }
  }
  
  /**
   * Open test output for reading.
   */
  public BufferedReader openOutput(String outputPath) throws IOException {
    return new BufferedReader(Channels.newReader(gcsUtil.open(GcsPath.fromUri(outputPath)), "UTF-8"));
  }

  /**
   * Delete test output.
   */
  public void deleteOutput(String outputPath) throws IOException, GeneralSecurityException {
    // boilerplate
    GcsPath path = GcsPath.fromUri(outputPath);
    GcsOptions gcsOptions = (GcsOptions)popts.as(GcsOptions.class);
    Storage storage = Transport.newStorageClient(gcsOptions).build();
    // do the actual work
    storage.objects().delete(path.getBucket(), path.getObject()).execute();
  }
  
}
