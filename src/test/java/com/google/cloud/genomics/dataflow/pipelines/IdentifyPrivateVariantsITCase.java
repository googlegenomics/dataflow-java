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
 *      mvn -Dit.test=IdentifyPrivateVariantsITCase#testLocal verify
 *
 * See also http://maven.apache.org/surefire/maven-failsafe-plugin/examples/single-test.html
 */
@RunWith(JUnit4.class)
public class IdentifyPrivateVariantsITCase {

  // This file contains mother, father, and children of CEPH pedigree 1463. The variants of
  // the grandparents are retained.
  static final String CALLSET_NAMES_FILEPATH = "src/test/resources/com/google/cloud/genomics/dataflow/pipelines/family.txt";

  static final String[] EXPECTED_RESULT = {
    "CI7s77ro84KpKhIFY2hyMTcYtcXSEyCZqdOrkK2qqqQB\tchr17\t41198261\t41198274\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcYwsXSEyDxqaydstXR8Rw\tchr17\t41198274\t41198290\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYmMXSEyCO2rS1w8z19ZQB\tchr17\t41198232\t41198261\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYxsXSEyC-2LnH-bPtvCs\tchr17\t41198278\t41198333\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYmMXSEyCWvJaK0Y2A0IgB\tchr17\t41198232\t41198240\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYoMXSEyCHrfj0jLu2ssgB\tchr17\t41198240\t41198263\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcYucXSEyDx9OHb9dXM5Ao\tchr17\t41198265\t41198278\tA\t",
    "CI7s77ro84KpKhIFY2hyMTcY0sXSEyCrq4PiocCNwTE\tchr17\t41198290\t41198323\tA\t"
  };

  static String outputPrefix;
  static IntegrationTestHelper helper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    helper = new IntegrationTestHelper();
    outputPrefix = helper.getTestOutputGcsFolder() + "identifyPrivateVariants";
  }

  @After
  public void tearDown() throws Exception {
    helper.deleteOutputs(outputPrefix);
  }

  @Test
  public void testLocal() throws Exception {
    String[] ARGS = {
        "--project=" + helper.getTestProject(),
        "--references=chr17:41198200:41198300", // smaller portion of BRCA1
        "--variantSetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--callSetNamesFilepath=" + CALLSET_NAMES_FILEPATH,
        "--output=" + outputPrefix,
        };

    System.out.println(ARGS);

    testBase(ARGS, EXPECTED_RESULT);
  }

  private void testBase(String[] ARGS, String[] expectedResult) throws Exception {
    // Run the pipeline.
    IdentifyPrivateVariants.main(ARGS);

    // Download the pipeline results.
    List<String> results = helper.downloadOutputs(outputPrefix, expectedResult.length);

    // Check the pipeline results.
    assertEquals(expectedResult.length, results.size());
    assertThat(results,
        CoreMatchers.allOf(CoreMatchers.hasItems(expectedResult)));
  }
}


