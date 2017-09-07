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
 *      mvn -Dit.test=IdentityByStateITCase#testLocal verify
 *
 * See also http://maven.apache.org/surefire/maven-failsafe-plugin/examples/single-test.html
 */
@RunWith(JUnit4.class)
public class IdentityByStateITCase {


  // There are only two SNPs in this region and everyone is called for both SNPs.
  // For https://github.com/googlegenomics/getting-started-bigquery/blob/master/sql/variant-level-data-for-brca1.sql
  // chr17   41196407    41196408    G   A       733.47  7
  // chr17   41196820    41196822    CT  C       63.74   1
  // chr17   41196820    41196823    CTT C,CT    314.59  3
  // chr17   41196840    41196841    G   T       85.68   2
  static final String SITES_FILEPATH = "src/test/resources/com/google/cloud/genomics/dataflow/pipelines/sites.tsv";

  static final String[] EXPECTED_SITES_RESULT = {
    "NA12877\tNA12878\t0.0\t0.0\t1",
    "NA12877\tNA12889\t0.0\t0.0\t1",
    "NA12877\tNA12890\t0.0\t0.0\t1",
    "NA12877\tNA12891\t0.0\t0.0\t1",
    "NA12877\tNA12892\t0.0\t0.0\t1",
    "NA12878\tNA12889\t1.0\t1.0\t1",
    "NA12878\tNA12890\t0.0\t0.0\t1",
    "NA12878\tNA12891\t0.0\t0.0\t1",
    "NA12878\tNA12892\t1.0\t1.0\t1",
    "NA12889\tNA12890\t0.0\t0.0\t1",
    "NA12889\tNA12891\t0.0\t0.0\t1",
    "NA12889\tNA12892\t1.0\t1.0\t1",
    "NA12890\tNA12891\t0.0\t0.0\t1",
    "NA12890\tNA12892\t0.0\t0.0\t1",
    "NA12891\tNA12892\t0.0\t0.0\t1",
  };

  static final String[] EXPECTED_BRCA1_RESULT = {
    "NA12877\tNA12878\t0.030303030303030304\t7.0\t231",
    "NA12877\tNA12889\t0.05194805194805195\t12.0\t231",
    "NA12877\tNA12890\t0.0603448275862069\t14.0\t232",
    "NA12877\tNA12891\t0.039647577092511016\t9.0\t227",
    "NA12877\tNA12892\t0.043478260869565216\t10.0\t230",
    "NA12878\tNA12889\t0.6244541484716157\t143.0\t229",
    "NA12878\tNA12890\t0.05217391304347826\t12.0\t230",
    "NA12878\tNA12891\t0.057777777777777775\t13.0\t225",
    "NA12878\tNA12892\t0.6244541484716157\t143.0\t229",
    "NA12889\tNA12890\t0.06086956521739131\t14.0\t230",
    "NA12889\tNA12891\t0.057777777777777775\t13.0\t225",
    "NA12889\tNA12892\t0.6550218340611353\t150.0\t229",
    "NA12890\tNA12891\t0.05752212389380531\t13.0\t226",
    "NA12890\tNA12892\t0.06956521739130435\t16.0\t230",
    "NA12891\tNA12892\t0.05357142857142857\t12.0\t224",
  };

  static String outputPrefix;
  static IntegrationTestHelper helper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    helper = new IntegrationTestHelper();
    outputPrefix = helper.getTestOutputGcsFolder() + "identityByState";
  }

  @After
  public void tearDown() throws Exception {
    helper.deleteOutputs(outputPrefix);
  }

  @Test
  public void testSitesFilepathLocal() throws Exception {

    String[] ARGS = {
        "--project=" + helper.getTestProject(),
        "--sitesFilepath=" + SITES_FILEPATH,
        "--variantSetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--hasNonVariantSegments",
        "--output=" + outputPrefix
        };
    testBase(ARGS, EXPECTED_SITES_RESULT);
  }

  @Test
  public void testLocal() throws Exception {
    String[] ARGS = {
        "--project=" + helper.getTestProject(),
        "--references=" + helper.PLATINUM_GENOMES_BRCA1_REFERENCES,
        "--variantSetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--hasNonVariantSegments",
        "--output=" + outputPrefix
        };
    testBase(ARGS, EXPECTED_BRCA1_RESULT);
  }

  private void testBase(String[] ARGS, String[] expectedResult) throws Exception {
    // Run the pipeline.
    IdentityByState.main(ARGS);

    // Download the pipeline results.
    List<String> results = helper.downloadOutputs(outputPrefix, expectedResult.length);

    assertEquals("Expected result length = " + expectedResult.length +
                 ", Actual result length = " + results.size(),
                 expectedResult.length, results.size());

    assertThat(results,
        CoreMatchers.allOf(CoreMatchers.hasItems(expectedResult)));
  }
}

