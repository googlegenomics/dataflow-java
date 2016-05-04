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

import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
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
public class IdentifyPrivateVariantsITCase {

  // This file contains mother, father, and children of CEPH pedigree 1463. The variants of
  // the grandparents are retained.
  static final String CALLSET_IDS_FILEPATH = "src/test/resources/com/google/cloud/genomics/dataflow/pipelines/family.txt";

  static final String[] EXPECTED_RESULT =
      {
    "CI7s77ro84KpKhIFY2hyMTcYncXSEyDZsv-QgbLW8LAB\tchr17\t41198237\t41198240\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYjcXSEyChrZ6a67-8gT4\tchr17\t41198221\t41198224\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcYusXSEyDZqKqB343JgJUB\tchr17\t41198266\t41198271\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcY0cXSEyDv8rDLgZaMl0s\tchr17\t41198289\t41198343\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYicXSEyCEydjKpbPTwBQ\tchr17\t41198217\t41198222\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcYuMXSEyC53bj3u7udl5AB\tchr17\t41198264\t41198284\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcYmMXSEyCO2rS1w8z19ZQB\tchr17\t41198232\t41198261\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcY1cXSEyDt0dbh97alkk8\tchr17\t41198293\t41198339\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYjMXSEyCmkM6HjOLVkV0\tchr17\t41198220\t41198221\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcY0sXSEyDVuJLlyrDP7v4B\tchr17\t41198290\t41198339\tA\t",
    "CI7s77ro84KpKhIFY2hyMTcYtcXSEyDFq7jnwoP063E\tchr17\t41198261\t41198265\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcYmMXSEyDP2ZG5prmy_h8\tchr17\t41198232\t41198264\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYscXSEyDu1v7C3uu5sDY\tchr17\t41198257\t41198264\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcY0sXSEyDenNPmycW46pQB\tchr17\t41198290\t41198292\tA\t",
    "CI7s77ro84KpKhIFY2hyMTcYmMXSEyDCjr-mmtasgfIB\tchr17\t41198232\t41198238\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYnsXSEyC2p5bZpaWQhwc\tchr17\t41198238\t41198239\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYjcXSEyCG0aq_i8yt_ZsB\tchr17\t41198221\t41198226\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcYn8XSEyDqivWRxY-r7bQB\tchr17\t41198239\t41198267\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcYmMXSEyCZ3-P-hbiB7G4\tchr17\t41198232\t41198237\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcY_MTSEyC8sO7OmMnE-6gB\tchr17\t41198204\t41198206\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcY-8TSEyCTwPGc8bO9vOgB\tchr17\t41198203\t41198204\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcY0sXSEyCrq4PiocCNwTE\tchr17\t41198290\t41198323\tA\t",
    "CI7s77ro84KpKhIFY2hyMTcYg8XSEyCDlOfIm_CVzaUB\tchr17\t41198211\t41198218\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcY_8TSEyCBwbnJncShwjQ\tchr17\t41198207\t41198210\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcYoMXSEyDK2rGA3OrXwnE\tchr17\t41198240\t41198261\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcYwsXSEyDxqaydstXR8Rw\tchr17\t41198274\t41198290\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYm8XSEyCkxu_n07Co7R8\tchr17\t41198235\t41198289\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcYjMXSEyDW-8LBy4Hd53Q\tchr17\t41198220\t41198221\tT\tG",
    "CI7s77ro84KpKhIFY2hyMTcYn8XSEyDX7bzU7bK6wRA\tchr17\t41198239\t41198266\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcY-sTSEyCU4IeF0NmG9FA\tchr17\t41198202\t41198203\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcY1MXSEyDRz_jF3d-AqpkB\tchr17\t41198292\t41198341\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYucXSEyC64fa-9JyMggc\tchr17\t41198265\t41198306\tA\t",
    "CI7s77ro84KpKhIFY2hyMTcYucXSEyCP6JTA5qGOiaIB\tchr17\t41198265\t41198305\tA\t",
    "CI7s77ro84KpKhIFY2hyMTcYucXSEyCxus-zh4-asDw\tchr17\t41198265\t41198276\tA\t",
    "CI7s77ro84KpKhIFY2hyMTcYjsXSEyDOgODrguDK5h4\tchr17\t41198222\t41198224\tA\t",
    "CI7s77ro84KpKhIFY2hyMTcYzsXSEyDu3ZfwkeWbZA\tchr17\t41198286\t41198303\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcYncXSEyCq4JWKmvHyv7kB\tchr17\t41198237\t41198239\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYysXSEyCO752Do_-3vcQB\tchr17\t41198282\t41198310\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcYi8XSEyCu3bC3wtK3n_cB\tchr17\t41198219\t41198220\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcYuMXSEyC445mtnZ_t2LIB\tchr17\t41198264\t41198268\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcYzMXSEyDExL3MiYa-yd8B\tchr17\t41198284\t41198332\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcYi8XSEyCytpywt6KD6-kB\tchr17\t41198219\t41198226\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcYuMXSEyCw-_2lwvLwsAc\tchr17\t41198264\t41198293\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcY08XSEyDW7Y2O96jhx2A\tchr17\t41198291\t41198342\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcYmMXSEyCO3bbH9ZDPvlo\tchr17\t41198232\t41198257\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYvMXSEyDgtv_d4orRwjo\tchr17\t41198268\t41198282\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYysXSEyD_kb3GkJKE510\tchr17\t41198282\t41198290\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcYu8XSEyDy_aja0u6Ts-kB\tchr17\t41198267\t41198290\tA\t",
    "CI7s77ro84KpKhIFY2hyMTcYucXSEyDE6KKR946HoEA\tchr17\t41198265\t41198343\tA\t",
    "CI7s77ro84KpKhIFY2hyMTcY-8TSEyDevLGmvZ3YsvQB\tchr17\t41198203\t41198220\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcYkMXSEyDY2L6Yt8_DsIUB\tchr17\t41198224\t41198226\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYtcXSEyCZqdOrkK2qqqQB\tchr17\t41198261\t41198274\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcY_sTSEyCjvZPHg4rO4g0\tchr17\t41198206\t41198207\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcYjcXSEyD62YylsrPL2ho\tchr17\t41198221\t41198222\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcYxMXSEyCL4tvunYeq7poB\tchr17\t41198276\t41198329\tT\t",
    "CI7s77ro84KpKhIFY2hyMTcYuMXSEyDh-4XT-NG3hAI\tchr17\t41198264\t41198286\tG\t",
    "CI7s77ro84KpKhIFY2hyMTcY0sXSEyCet96DzLOr18YB\tchr17\t41198290\t41198291\tA\t",
    "CI7s77ro84KpKhIFY2hyMTcYisXSEyD7qJX59MWviEc\tchr17\t41198218\t41198226\tC\t",
    "CI7s77ro84KpKhIFY2hyMTcYucXSEyChw-zynrLA99oB\tchr17\t41198265\t41198290\tA\t",
    "CI7s77ro84KpKhIFY2hyMTcYwMXSEyC-8fOf8MSLmIAB\tchr17\t41198272\t41198282\tG\t",      };

  static String outputPrefix;
  static IntegrationTestHelper helper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    helper = new IntegrationTestHelper();
    outputPrefix = helper.getTestOutputGcsFolder() + "identifyPrivateVariants";
  }

  @After
  public void tearDown() throws Exception {
    for (GcsPath path : helper.gcsUtil.expand(GcsPath.fromUri(outputPrefix + "*"))) {
      helper.deleteOutput(path.toString());
    }
  }

  @Test
  public void testLocal() throws Exception {
    String[] ARGS = {
        "--references=chr17:41198200:41198300", // smaller portion of BRCA1
        "--variantSetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--callSetIdsFilepath=" + CALLSET_IDS_FILEPATH,
        "--output=" + outputPrefix,
        };

    System.out.println(ARGS);

    testBase(ARGS, EXPECTED_RESULT);
  }

  private void testBase(String[] ARGS, String[] expectedResult) throws Exception {
    // Run the pipeline.
    IdentifyPrivateVariants.main(ARGS);

    // Download the pipeline results.
    List<String> results = Lists.newArrayList();
    for (GcsPath path : helper.gcsUtil.expand(GcsPath.fromUri(outputPrefix + "*"))) {
      BufferedReader reader = helper.openOutput(path.toString());
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        results.add(line);
      }
    }

    // Check the pipeline results.
    assertThat(results,
        CoreMatchers.allOf(CoreMatchers.hasItems(expectedResult)));
  }
}


