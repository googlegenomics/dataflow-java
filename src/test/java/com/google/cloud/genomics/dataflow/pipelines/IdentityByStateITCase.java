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
import java.util.List;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;

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
 *      mvn -Dit.test=IdentityByStateITCase#testPaginatedLocal verify
 *
 * See also http://maven.apache.org/surefire/maven-failsafe-plugin/examples/single-test.html
 */
public class IdentityByStateITCase {

  static final String[] EXPECTED_RESULT = {
      "NA12877	NA12893	0.047619047619047616	13.0	273",
      "NA12878	NA12888	0.5793103448275863	168.0	290",
      "NA12879	NA12884	0.0683453237410072	19.0	278",
      "NA12887	NA12888	0.5979381443298969	174.0	291",
      "NA12890	NA12893	0.07063197026022305	19.0	269",
      "NA12884	NA12885	0.06498194945848375	18.0	277",
      "NA12885	NA12887	0.06934306569343066	19.0	274",
      "NA12881	NA12890	0.08088235294117647	22.0	272",
      "NA12877	NA12884	0.04	11.0	275",
      "NA12881	NA12887	0.07011070110701106	19.0	271",
      "NA12882	NA12884	0.06498194945848375	18.0	277",
      "NA12877	NA12882	0.04710144927536232	13.0	276",
      "NA12881	NA12889	0.06593406593406594	18.0	273",
      "NA12877	NA12885	0.05054151624548736	14.0	277",
      "NA12890	NA12892	0.05925925925925926	16.0	270",
      "NA12887	NA12892	0.6068965517241379	176.0	290",
      "NA12880	NA12887	0.5874125874125874	168.0	286",
      "NA12880	NA12881	0.07037037037037037	19.0	270",
      "NA12886	NA12891	0.05204460966542751	14.0	269",
      "NA12878	NA12881	0.05128205128205128	14.0	273",
      "NA12888	NA12890	0.05970149253731343	16.0	268",
      "NA12883	NA12884	0.03018867924528302	8.0	265",
      "NA12881	NA12883	0.052830188679245285	14.0	265",
      "NA12879	NA12881	0.06498194945848375	18.0	277",
      "NA12881	NA12893	0.09523809523809523	26.0	273",
      "NA12888	NA12891	0.04887218045112782	13.0	266",
      "NA12889	NA12891	0.04868913857677903	13.0	267",
      "NA12877	NA12890	0.051470588235294115	14.0	272",
      "NA12884	NA12891	0.037037037037037035	10.0	270",
      "NA12877	NA12891	0.037037037037037035	10.0	270",
      "NA12880	NA12884	0.03717472118959108	10.0	269",
      "NA12886	NA12893	0.055350553505535055	15.0	271",
      "NA12883	NA12887	0.5915492957746479	168.0	284",
      "NA12886	NA12887	0.05925925925925926	16.0	270",
      "NA12881	NA12892	0.0695970695970696	19.0	273",
      "NA12884	NA12889	0.040293040293040296	11.0	273",
      "NA12877	NA12878	0.02564102564102564	7.0	273",
      "NA12884	NA12893	0.04797047970479705	13.0	271",
      "NA12877	NA12888	0.05860805860805861	16.0	273",
      "NA12882	NA12893	0.040293040293040296	11.0	273",
      "NA12878	NA12890	0.044444444444444446	12.0	270",
      "NA12885	NA12893	0.06227106227106227	17.0	273",
      "NA12879	NA12887	0.0695970695970696	19.0	273",
      "NA12878	NA12886	0.03676470588235294	10.0	272",
      "NA12882	NA12891	0.04044117647058824	11.0	272",
      "NA12887	NA12890	0.056179775280898875	15.0	267",
      "NA12878	NA12892	0.5601374570446735	163.0	291",
      "NA12883	NA12891	0.03861003861003861	10.0	259",
      "NA12882	NA12890	0.04059040590405904	11.0	271",
      "NA12877	NA12880	0.044444444444444446	12.0	270",
      "NA12885	NA12888	0.06909090909090909	19.0	275",
      "NA12878	NA12884	0.03296703296703297	9.0	273",
      "NA12882	NA12886	0.05090909090909091	14.0	275",
      "NA12888	NA12892	0.5898305084745763	174.0	295",
      "NA12883	NA12890	0.04905660377358491	13.0	265",
      "NA12886	NA12888	0.04797047970479705	13.0	271",
      "NA12877	NA12883	0.026415094339622643	7.0	265",
      "NA12881	NA12884	0.06181818181818182	17.0	275",
      "NA12878	NA12879	0.043795620437956206	12.0	274",
      "NA12880	NA12883	0.578397212543554	166.0	287",
      "NA12884	NA12892	0.04395604395604396	12.0	273",
      "NA12883	NA12889	0.5669014084507042	161.0	284",
      "NA12882	NA12892	0.051094890510948905	14.0	274",
      "NA12879	NA12888	0.07664233576642336	21.0	274",
      "NA12885	NA12889	0.05454545454545454	15.0	275",
      "NA12888	NA12889	0.5945017182130584	173.0	291",
      "NA12880	NA12889	0.5664335664335665	162.0	286",
      "NA12879	NA12885	0.06451612903225806	18.0	279",
      "NA12886	NA12892	0.03676470588235294	10.0	272",
      "NA12877	NA12886	0.04363636363636364	12.0	275",
      "NA12880	NA12891	0.045454545454545456	12.0	264",
      "NA12883	NA12893	0.060836501901140684	16.0	263",
      "NA12877	NA12879	0.05415162454873646	15.0	277",
      "NA12878	NA12887	0.5679442508710801	163.0	287",
      "NA12879	NA12882	0.06093189964157706	17.0	279",
      "NA12880	NA12888	0.5818815331010453	167.0	287",
      "NA12877	NA12887	0.05514705882352941	15.0	272",
      "NA12892	NA12893	0.06666666666666667	18.0	270",
      "NA12887	NA12893	0.0661764705882353	18.0	272",
      "NA12889	NA12893	0.05925925925925926	16.0	270",
      "NA12877	NA12889	0.04744525547445255	13.0	274",
      "NA12878	NA12889	0.5570934256055363	161.0	289",
      "NA12877	NA12881	0.050724637681159424	14.0	276",
      "NA12879	NA12889	0.05818181818181818	16.0	275",
      "NA12882	NA12887	0.06593406593406594	18.0	273",
      "NA12883	NA12886	0.03018867924528302	8.0	265",
      "NA12878	NA12880	0.5724137931034483	166.0	290",
      "NA12878	NA12883	0.5574912891986062	160.0	287",
      "NA12878	NA12893	0.05185185185185185	14.0	270",
      "NA12879	NA12892	0.05818181818181818	16.0	275",
      "NA12882	NA12885	0.06474820143884892	18.0	278",
      "NA12882	NA12889	0.043795620437956206	12.0	274",
      "NA12885	NA12886	0.04710144927536232	13.0	276",
      "NA12881	NA12885	0.06859205776173286	19.0	277",
      "NA12891	NA12893	0.07037037037037037	19.0	270",
      "NA12885	NA12890	0.04779411764705882	13.0	272",
      "NA12879	NA12893	0.0695970695970696	19.0	273",
      "NA12883	NA12885	0.03018867924528302	8.0	265",
      "NA12882	NA12888	0.06227106227106227	17.0	273",
      "NA12878	NA12882	0.03663003663003663	10.0	273",
      "NA12886	NA12890	0.04814814814814815	13.0	270",
      "NA12882	NA12883	0.026515151515151516	7.0	264",
      "NA12884	NA12886	0.05776173285198556	16.0	277",
      "NA12884	NA12887	0.06642066420664207	18.0	271",
      "NA12879	NA12880	0.04797047970479705	13.0	271",
      "NA12883	NA12892	0.5729166666666666	165.0	288",
      "NA12889	NA12892	0.5787671232876712	169.0	292",
      "NA12880	NA12890	0.05925925925925926	16.0	270",
      "NA12881	NA12891	0.08118081180811808	22.0	271",
      "NA12884	NA12888	0.05514705882352941	15.0	272",
      "NA12880	NA12882	0.040740740740740744	11.0	270",
      "NA12880	NA12886	0.041044776119402986	11.0	268",
      "NA12879	NA12883	0.022641509433962263	6.0	265",
      "NA12887	NA12891	0.06343283582089553	17.0	268",
      "NA12880	NA12893	0.0599250936329588	16.0	267",
      "NA12880	NA12885	0.04428044280442804	12.0	271",
      "NA12879	NA12886	0.05434782608695652	15.0	276",
      "NA12880	NA12892	0.5655172413793104	164.0	290",
      "NA12891	NA12892	0.04868913857677903	13.0	267",
      "NA12883	NA12888	0.5614035087719298	160.0	285",
      "NA12881	NA12888	0.07352941176470588	20.0	272",
      "NA12881	NA12886	0.06545454545454546	18.0	275",
      "NA12889	NA12890	0.05185185185185185	14.0	270",
      "NA12884	NA12890	0.037037037037037035	10.0	270",
      "NA12886	NA12889	0.051470588235294115	14.0	272",
      "NA12885	NA12892	0.05818181818181818	16.0	275",
      "NA12881	NA12882	0.050724637681159424	14.0	276",
      "NA12890	NA12891	0.04887218045112782	13.0	266",
      "NA12879	NA12891	0.058823529411764705	16.0	272",
      "NA12887	NA12889	0.5778546712802768	167.0	289",
      "NA12878	NA12891	0.05223880597014925	14.0	268",
      "NA12878	NA12885	0.03663003663003663	10.0	273",
      "NA12877	NA12892	0.043795620437956206	12.0	274",
      "NA12885	NA12891	0.04428044280442804	12.0	271",
      "NA12879	NA12890	0.04411764705882353	12.0	272",
      "NA12888	NA12893	0.06319702602230483	17.0	269"
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
    for (GcsPath path : helper.gcsUtil.expand(GcsPath.fromUri(outputPrefix + "*"))) {
      helper.deleteOutput(path.toString());
    }
  }

  @Test
  public void testPaginatedLocal() throws Exception {
    String[] ARGS = {
        "--references=" + helper.PLATINUM_GENOMES_BRCA1_REFERENCES,
        "--variantSetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--hasNonVariantSegments",
        "--output=" + outputPrefix,
        "--useGrpc=false"
        };
    testBase(ARGS);
  }

  @Test
  public void testPaginatedCloud() throws Exception {
    String[] ARGS = {
        "--references=" + helper.PLATINUM_GENOMES_BRCA1_REFERENCES,
        "--variantSetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--hasNonVariantSegments",
        "--output=" + outputPrefix,
        "--project=" + helper.getTestProject(),
        "--runner=BlockingDataflowPipelineRunner",
        "--stagingLocation=" + helper.getTestStagingGcsFolder(),
        "--useGrpc=false"
        };
    testBase(ARGS);
  }

  @Test
  public void testStreamingLocal() throws Exception {
    String[] ARGS = {
        "--references=" + helper.PLATINUM_GENOMES_BRCA1_REFERENCES,
        "--variantSetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--hasNonVariantSegments",
        "--output=" + outputPrefix,
        "--useGrpc=true"
        };
    testBase(ARGS);
  }

  @Test
  public void testStreamingCloud() throws Exception {
    String[] ARGS = {
        "--references=" + helper.PLATINUM_GENOMES_BRCA1_REFERENCES,
        "--variantSetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--hasNonVariantSegments",
        "--output=" + outputPrefix,
        "--project=" + helper.getTestProject(),
        "--runner=BlockingDataflowPipelineRunner",
        "--stagingLocation=" + helper.getTestStagingGcsFolder(),
        "--useGrpc=true"
        };
    testBase(ARGS);
  }

  private void testBase(String[] ARGS) throws Exception {
    // Run the pipeline.
    IdentityByState.main(ARGS);

    // Download the pipeline results.
    List<String> results = Lists.newArrayList();
    for (GcsPath path : helper.gcsUtil.expand(GcsPath.fromUri(outputPrefix + "*"))) {
      BufferedReader reader = helper.openOutput(path.toString());
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        results.add(line);
      }
    }

    assertEquals("Expected result length = " + EXPECTED_RESULT.length +
                 ", Actual result length = " + results.size(),
                 EXPECTED_RESULT.length, results.size());

    assertThat(results,
        CoreMatchers.allOf(CoreMatchers.hasItems(EXPECTED_RESULT)));
  }
}
