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
    new GraphResult("NA12892", -0.6, -0.07),
    new GraphResult("NA12887", -0.6, -0.07),
    new GraphResult("NA12880", -0.6, -0.07),
    new GraphResult("NA12877", 0.35, 0.23),
    new GraphResult("NA12878", -0.6, -0.07),
    new GraphResult("NA12889", -0.6, -0.07),
    new GraphResult("NA12888", -0.6, -0.07),
    new GraphResult("NA12879", 0.65, -0.72),
    new GraphResult("NA12881", 0.35, 0.23),
    new GraphResult("NA12885", 0.35, 0.23),
    new GraphResult("NA12891", 0.35, 0.23),
    new GraphResult("NA12884", 0.35, 0.23),
    new GraphResult("NA12883", -0.6, -0.07),
    new GraphResult("NA12886", 0.35, 0.23),
    new GraphResult("NA12893", 0.65, -0.72),
    new GraphResult("NA12882", 0.35, 0.23),
    new GraphResult("NA12890", 0.35, 0.23)
  };

  static final GraphResult[] EXPECTED_BRCA1_RESULT = {
    new GraphResult("NA12877", 5.18, 0.22),
    new GraphResult("NA12878", -7.39, -1.7),
    new GraphResult("NA12879", 5.26, 1.37),
    new GraphResult("NA12880", -7.41, -2.74),
    new GraphResult("NA12881", 5.27, -1.06),
    new GraphResult("NA12882", 5.21, 1.19),
    new GraphResult("NA12883", -7.57, -3.73),
    new GraphResult("NA12884", 5.33, 0.95),
    new GraphResult("NA12885", 5.21, 1.07),
    new GraphResult("NA12886", 5.28, -0.15),
    new GraphResult("NA12887", -7.44, 1.69),
    new GraphResult("NA12888", -7.47, 2.72),
    new GraphResult("NA12889", -7.34, 1.65),
    new GraphResult("NA12890", 5.04, -1.61),
    new GraphResult("NA12891", 5.24, -0.88),
    new GraphResult("NA12892", -7.64, 2.1),
    new GraphResult("NA12893", 5.18, -1.18)
  };

  static final GraphResult[] EXPECTED_CALLSETS_RESULT = {
    new GraphResult("NA12877", 4.58, 2.63),
    new GraphResult("NA12880", -9.1, 0.01),
    new GraphResult("NA12890", 4.5, -2.66)
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


