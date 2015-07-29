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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.api.client.util.Lists;
import com.google.cloud.genomics.dataflow.functions.PCoAnalysis.GraphResult;

/**
 * This integration test will call the Genomics API and write to Cloud Storage.
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
 *      mvn -Dit.test=VariantSimilarityITCase#testLocal verify
 *      
 * See also http://maven.apache.org/surefire/maven-failsafe-plugin/examples/single-test.html
 */
public class VariantSimilarityITCase {
  
  static final String OUTPUT_SUFFIX = "-00000-of-00001";
  
  static String outputPrefix;
  static IntegrationTestHelper helper;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    helper = new IntegrationTestHelper();
    outputPrefix = helper.TEST_OUTPUT_GCS_FOLDER + "variantSimilarity";
  }

  @Before
  public void setUp() throws Exception {
    helper.touchOutput(outputPrefix + OUTPUT_SUFFIX);
  }

  @After
  public void tearDown() throws Exception {
    helper.deleteOutput(outputPrefix + OUTPUT_SUFFIX);
  }

  @Test
  public void testLocal() throws IOException, GeneralSecurityException {
    String[] ARGS = {
        "--apiKey=" + helper.API_KEY,
        "--references=" + helper.PLATINUM_GENOMES_BRCA1_REFERENCES,
        "--datasetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--output=" + outputPrefix,
        };
    testBase(ARGS);
  }

  @Test
  public void testCloud() throws IOException, GeneralSecurityException {
    String[] ARGS = {
        "--apiKey=" + helper.API_KEY,
        "--references=" + helper.PLATINUM_GENOMES_BRCA1_REFERENCES,
        "--datasetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--output=" + outputPrefix,
        "--project=" + helper.TEST_PROJECT,
        "--numWorkers=1",  // Use only a single worker to ensure a single output file.
        "--runner=BlockingDataflowPipelineRunner",
        "--stagingLocation=" + helper.TEST_STAGING_GCS_FOLDER,
        };
    testBase(ARGS);
  }
  
  private void testBase(String[] ARGS) throws IOException, GeneralSecurityException {
    // Run the pipeline.
    VariantSimilarity.main(ARGS);
   
    // Download the pipeline results.
    BufferedReader reader = helper.openOutput(outputPrefix + OUTPUT_SUFFIX);
    List<GraphResult> results = Lists.newArrayList();
    for (String line = reader.readLine(); line != null; line = reader.readLine()) {
      results.add(GraphResult.fromString(line));
    }

    // Check the pipeline results.
    assertEquals(helper.PLATINUM_GENOMES_NUMBER_OF_SAMPLES, results.size());
    
    /* TODO place a stable sort on the samples to achieve reproducible results
    assertThat(results, 
        IsIterableContainingInAnyOrder.containsInAnyOrder(
            new GraphResult("NA12887", 7.43, -1.7),
            new GraphResult("NA12886", -5.29, 0.13),
            new GraphResult("NA12885", -5.22, -1.08),
            new GraphResult("NA12878", 7.38, 1.69),
            new GraphResult("NA12884", -5.34, -0.96),
            new GraphResult("NA12883", 7.56, 3.72),
            new GraphResult("NA12881", -5.28, 1.05),
            new GraphResult("NA12888", 7.46, -2.73),
            new GraphResult("NA12889", 7.33, -1.66),
            new GraphResult("NA12891", -5.25, 0.87),
            new GraphResult("NA12882", -5.22, -1.2),
            new GraphResult("NA12892", 7.63, -2.11),
            new GraphResult("NA12890", -5.05, 1.6),
            new GraphResult("NA12877", -5.19, -0.23),
            new GraphResult("NA12893", -5.19, 1.17),
            new GraphResult("NA12880", 7.4, 2.73),
            new GraphResult("NA12879", -5.27, -1.38)));
     */
  }
}


