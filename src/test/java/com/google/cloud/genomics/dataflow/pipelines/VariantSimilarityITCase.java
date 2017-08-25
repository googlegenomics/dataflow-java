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

import com.google.api.client.util.Lists;
import com.google.cloud.genomics.dataflow.functions.pca.PCoAnalysis.GraphResult;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

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
@RunWith(JUnit4.class)
public class VariantSimilarityITCase {

  static final GraphResult[] EXPECTED_SITES_RESULT = {
    new GraphResult("NA12892", 0.5, -0.01),
    new GraphResult("NA12877", -0.5, -0.01),
    new GraphResult("NA12878", 0.5, 0.0),
    new GraphResult("NA12889", 0.5, 0.0),
    new GraphResult("NA12891", -0.51, 0.0),
    new GraphResult("NA12890", -0.5, -0.01)
  };

  static final GraphResult[] EXPECTED_BRCA1_RESULT = {
    new GraphResult("NA12877", -6.51, -0.52),
    new GraphResult("NA12878", 6.36, 4.89),
    new GraphResult("NA12889", 6.37, -1.17),
    new GraphResult("NA12890", -6.5, -0.36),
    new GraphResult("NA12891", -6.51, 0.75),
    new GraphResult("NA12892", 6.75, -3.63),
  };

  static final GraphResult[] EXPECTED_CALLSETS_RESULT = {
    new GraphResult("NA12877", 4.45, 2.66),
    new GraphResult("NA12889", -9.17, -0.05),
    new GraphResult("NA12890", 4.7, -2.62)
  };

  static String outputPrefix;
  static IntegrationTestHelper helper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    helper = new IntegrationTestHelper();
    outputPrefix = helper.getTestOutputGcsFolder() + "variantSimilarity";
  }

  @After
  public void tearDown() throws Exception {
    helper.deleteOutputs(outputPrefix);
  }

  @Test
  public void testLocal() throws Exception {
    String[] ARGS = {
        "--project=" + helper.getTestProject(),
        "--references=" + helper.PLATINUM_GENOMES_BRCA1_REFERENCES,
        "--variantSetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--output=" + outputPrefix,
        };
    testBase(ARGS, EXPECTED_BRCA1_RESULT);
  }

  @Test
  public void testSitesFilepathLocal() throws Exception {
    String[] ARGS = {
        "--project=" + helper.getTestProject(),
        "--sitesFilepath=" + IdentityByStateITCase.SITES_FILEPATH,
        "--variantSetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--output=" + outputPrefix,
        };
    testBase(ARGS, EXPECTED_SITES_RESULT);
  }

  @Test
  public void testCallSetsLocal() throws Exception {
    String[] ARGS = {
        "--project=" + helper.getTestProject(),
        "--references=" + helper.PLATINUM_GENOMES_BRCA1_REFERENCES,
        "--variantSetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--callSetNames=" + helper.A_FEW_PLATINUM_GENOMES_CALLSET_NAMES,
        "--output=" + outputPrefix,
        };
    testBase(ARGS, EXPECTED_CALLSETS_RESULT);
  }

  @Test
  public void testCloud() throws Exception {
    String[] ARGS = {
        "--project=" + helper.getTestProject(),
        "--references=" + helper.PLATINUM_GENOMES_BRCA1_REFERENCES,
        "--variantSetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--output=" + outputPrefix,
        "--runner=BlockingDataflowPipelineRunner",
        "--stagingLocation=" + helper.getTestStagingGcsFolder(),
        };
    testBase(ARGS, EXPECTED_BRCA1_RESULT);
  }

  private void testBase(String[] ARGS, GraphResult[] expectedResult) throws Exception {
    // Run the pipeline.
    VariantSimilarity.main(ARGS);

    // Download the pipeline results.
    List<String> rawResults = helper.downloadOutputs(outputPrefix, expectedResult.length);
    List<GraphResult> results = Lists.newArrayList();
    for (String result : rawResults) {
      results.add(GraphResult.fromString(result));
    }

    // Check the pipeline results.
    assertEquals(expectedResult.length, results.size());

    assertThat(results,
        CoreMatchers.allOf(CoreMatchers.hasItems(expectedResult)));
  }
}


