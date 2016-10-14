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
    "NA12877\tNA12878\t0.0\t0.0\t2",
    "NA12877\tNA12879\t0.0\t0.0\t2",
    "NA12877\tNA12880\t0.0\t0.0\t2",
    "NA12877\tNA12881\t0.0\t0.0\t2",
    "NA12877\tNA12882\t0.0\t0.0\t2",
    "NA12877\tNA12883\t0.0\t0.0\t2",
    "NA12877\tNA12884\t0.0\t0.0\t2",
    "NA12877\tNA12885\t0.0\t0.0\t2",
    "NA12877\tNA12886\t0.0\t0.0\t2",
    "NA12877\tNA12887\t0.0\t0.0\t2",
    "NA12877\tNA12888\t0.0\t0.0\t2",
    "NA12877\tNA12889\t0.0\t0.0\t2",
    "NA12877\tNA12890\t0.0\t0.0\t2",
    "NA12877\tNA12891\t0.0\t0.0\t2",
    "NA12877\tNA12892\t0.0\t0.0\t2",
    "NA12877\tNA12893\t0.0\t0.0\t2",
    "NA12878\tNA12879\t0.0\t0.0\t2",
    "NA12878\tNA12880\t0.5\t1.0\t2",
    "NA12878\tNA12881\t0.0\t0.0\t2",
    "NA12878\tNA12882\t0.0\t0.0\t2",
    "NA12878\tNA12883\t0.5\t1.0\t2",
    "NA12878\tNA12884\t0.0\t0.0\t2",
    "NA12878\tNA12885\t0.0\t0.0\t2",
    "NA12878\tNA12886\t0.0\t0.0\t2",
    "NA12878\tNA12887\t0.5\t1.0\t2",
    "NA12878\tNA12888\t0.5\t1.0\t2",
    "NA12878\tNA12889\t0.5\t1.0\t2",
    "NA12878\tNA12890\t0.0\t0.0\t2",
    "NA12878\tNA12891\t0.0\t0.0\t2",
    "NA12878\tNA12892\t0.5\t1.0\t2",
    "NA12878\tNA12893\t0.0\t0.0\t2",
    "NA12879\tNA12880\t0.0\t0.0\t2",
    "NA12879\tNA12881\t0.0\t0.0\t2",
    "NA12879\tNA12882\t0.0\t0.0\t2",
    "NA12879\tNA12883\t0.0\t0.0\t2",
    "NA12879\tNA12884\t0.0\t0.0\t2",
    "NA12879\tNA12885\t0.0\t0.0\t2",
    "NA12879\tNA12886\t0.0\t0.0\t2",
    "NA12879\tNA12887\t0.0\t0.0\t2",
    "NA12879\tNA12888\t0.0\t0.0\t2",
    "NA12879\tNA12889\t0.0\t0.0\t2",
    "NA12879\tNA12890\t0.0\t0.0\t2",
    "NA12879\tNA12891\t0.0\t0.0\t2",
    "NA12879\tNA12892\t0.0\t0.0\t2",
    "NA12879\tNA12893\t0.5\t1.0\t2",
    "NA12880\tNA12881\t0.0\t0.0\t2",
    "NA12880\tNA12882\t0.0\t0.0\t2",
    "NA12880\tNA12883\t0.5\t1.0\t2",
    "NA12880\tNA12884\t0.0\t0.0\t2",
    "NA12880\tNA12885\t0.0\t0.0\t2",
    "NA12880\tNA12886\t0.0\t0.0\t2",
    "NA12880\tNA12887\t0.5\t1.0\t2",
    "NA12880\tNA12888\t0.5\t1.0\t2",
    "NA12880\tNA12889\t0.5\t1.0\t2",
    "NA12880\tNA12890\t0.0\t0.0\t2",
    "NA12880\tNA12891\t0.0\t0.0\t2",
    "NA12880\tNA12892\t0.5\t1.0\t2",
    "NA12880\tNA12893\t0.0\t0.0\t2",
    "NA12881\tNA12882\t0.0\t0.0\t2",
    "NA12881\tNA12883\t0.0\t0.0\t2",
    "NA12881\tNA12884\t0.0\t0.0\t2",
    "NA12881\tNA12885\t0.0\t0.0\t2",
    "NA12881\tNA12886\t0.0\t0.0\t2",
    "NA12881\tNA12887\t0.0\t0.0\t2",
    "NA12881\tNA12888\t0.0\t0.0\t2",
    "NA12881\tNA12889\t0.0\t0.0\t2",
    "NA12881\tNA12890\t0.0\t0.0\t2",
    "NA12881\tNA12891\t0.0\t0.0\t2",
    "NA12881\tNA12892\t0.0\t0.0\t2",
    "NA12881\tNA12893\t0.0\t0.0\t2",
    "NA12882\tNA12883\t0.0\t0.0\t2",
    "NA12882\tNA12884\t0.0\t0.0\t2",
    "NA12882\tNA12885\t0.0\t0.0\t2",
    "NA12882\tNA12886\t0.0\t0.0\t2",
    "NA12882\tNA12887\t0.0\t0.0\t2",
    "NA12882\tNA12888\t0.0\t0.0\t2",
    "NA12882\tNA12889\t0.0\t0.0\t2",
    "NA12882\tNA12890\t0.0\t0.0\t2",
    "NA12882\tNA12891\t0.0\t0.0\t2",
    "NA12882\tNA12892\t0.0\t0.0\t2",
    "NA12882\tNA12893\t0.0\t0.0\t2",
    "NA12883\tNA12884\t0.0\t0.0\t2",
    "NA12883\tNA12885\t0.0\t0.0\t2",
    "NA12883\tNA12886\t0.0\t0.0\t2",
    "NA12883\tNA12887\t0.5\t1.0\t2",
    "NA12883\tNA12888\t0.5\t1.0\t2",
    "NA12883\tNA12889\t0.5\t1.0\t2",
    "NA12883\tNA12890\t0.0\t0.0\t2",
    "NA12883\tNA12891\t0.0\t0.0\t2",
    "NA12883\tNA12892\t0.5\t1.0\t2",
    "NA12883\tNA12893\t0.0\t0.0\t2",
    "NA12884\tNA12885\t0.0\t0.0\t2",
    "NA12884\tNA12886\t0.0\t0.0\t2",
    "NA12884\tNA12887\t0.0\t0.0\t2",
    "NA12884\tNA12888\t0.0\t0.0\t2",
    "NA12884\tNA12889\t0.0\t0.0\t2",
    "NA12884\tNA12890\t0.0\t0.0\t2",
    "NA12884\tNA12891\t0.0\t0.0\t2",
    "NA12884\tNA12892\t0.0\t0.0\t2",
    "NA12884\tNA12893\t0.0\t0.0\t2",
    "NA12885\tNA12886\t0.0\t0.0\t2",
    "NA12885\tNA12887\t0.0\t0.0\t2",
    "NA12885\tNA12888\t0.0\t0.0\t2",
    "NA12885\tNA12889\t0.0\t0.0\t2",
    "NA12885\tNA12890\t0.0\t0.0\t2",
    "NA12885\tNA12891\t0.0\t0.0\t2",
    "NA12885\tNA12892\t0.0\t0.0\t2",
    "NA12885\tNA12893\t0.0\t0.0\t2",
    "NA12886\tNA12887\t0.0\t0.0\t2",
    "NA12886\tNA12888\t0.0\t0.0\t2",
    "NA12886\tNA12889\t0.0\t0.0\t2",
    "NA12886\tNA12890\t0.0\t0.0\t2",
    "NA12886\tNA12891\t0.0\t0.0\t2",
    "NA12886\tNA12892\t0.0\t0.0\t2",
    "NA12886\tNA12893\t0.0\t0.0\t2",
    "NA12887\tNA12888\t0.5\t1.0\t2",
    "NA12887\tNA12889\t0.5\t1.0\t2",
    "NA12887\tNA12890\t0.0\t0.0\t2",
    "NA12887\tNA12891\t0.0\t0.0\t2",
    "NA12887\tNA12892\t0.5\t1.0\t2",
    "NA12887\tNA12893\t0.0\t0.0\t2",
    "NA12888\tNA12889\t0.5\t1.0\t2",
    "NA12888\tNA12890\t0.0\t0.0\t2",
    "NA12888\tNA12891\t0.0\t0.0\t2",
    "NA12888\tNA12892\t0.5\t1.0\t2",
    "NA12888\tNA12893\t0.0\t0.0\t2",
    "NA12889\tNA12890\t0.0\t0.0\t2",
    "NA12889\tNA12891\t0.0\t0.0\t2",
    "NA12889\tNA12892\t0.5\t1.0\t2",
    "NA12889\tNA12893\t0.0\t0.0\t2",
    "NA12890\tNA12891\t0.0\t0.0\t2",
    "NA12890\tNA12892\t0.0\t0.0\t2",
    "NA12890\tNA12893\t0.0\t0.0\t2",
    "NA12891\tNA12892\t0.0\t0.0\t2",
    "NA12891\tNA12893\t0.0\t0.0\t2",
    "NA12892\tNA12893\t0.0\t0.0\t2",
      };

  static final String[] EXPECTED_BRCA1_RESULT = {
    "NA12877\tNA12878\t0.025735294117647058\t7.0\t272",
    "NA12877\tNA12879\t0.04744525547445255\t13.0\t274",
    "NA12877\tNA12880\t0.04460966542750929\t12.0\t269",
    "NA12877\tNA12881\t0.04744525547445255\t13.0\t274",
    "NA12877\tNA12882\t0.04395604395604396\t12.0\t273",
    "NA12877\tNA12883\t0.026515151515151516\t7.0\t264",
    "NA12877\tNA12884\t0.03676470588235294\t10.0\t272",
    "NA12877\tNA12885\t0.04744525547445255\t13.0\t274",
    "NA12877\tNA12886\t0.04044117647058824\t11.0\t272",
    "NA12877\tNA12887\t0.048327137546468404\t13.0\t269",
    "NA12877\tNA12888\t0.05185185185185185\t14.0\t270",
    "NA12877\tNA12889\t0.04411764705882353\t12.0\t272",
    "NA12877\tNA12890\t0.05166051660516605\t14.0\t271",
    "NA12877\tNA12891\t0.033582089552238806\t9.0\t268",
    "NA12877\tNA12892\t0.03690036900369004\t10.0\t271",
    "NA12877\tNA12893\t0.04428044280442804\t12.0\t271",
    "NA12878\tNA12879\t0.04411764705882353\t12.0\t272",
    "NA12878\tNA12880\t0.5427509293680297\t146.0\t269",
    "NA12878\tNA12881\t0.051470588235294115\t14.0\t272",
    "NA12878\tNA12882\t0.03690036900369004\t10.0\t271",
    "NA12878\tNA12883\t0.5227272727272727\t138.0\t264",
    "NA12878\tNA12884\t0.033210332103321034\t9.0\t271",
    "NA12878\tNA12885\t0.03676470588235294\t10.0\t272",
    "NA12878\tNA12886\t0.03690036900369004\t10.0\t271",
    "NA12878\tNA12887\t0.5410447761194029\t145.0\t268",
    "NA12878\tNA12888\t0.550185873605948\t148.0\t269",
    "NA12878\tNA12889\t0.5296296296296297\t143.0\t270",
    "NA12878\tNA12890\t0.04460966542750929\t12.0\t269",
    "NA12878\tNA12891\t0.04887218045112782\t13.0\t266",
    "NA12878\tNA12892\t0.5296296296296297\t143.0\t270",
    "NA12878\tNA12893\t0.05204460966542751\t14.0\t269",
    "NA12879\tNA12880\t0.04460966542750929\t12.0\t269",
    "NA12879\tNA12881\t0.058394160583941604\t16.0\t274",
    "NA12879\tNA12882\t0.05128205128205128\t14.0\t273",
    "NA12879\tNA12883\t0.022727272727272728\t6.0\t264",
    "NA12879\tNA12884\t0.058823529411764705\t16.0\t272",
    "NA12879\tNA12885\t0.05474452554744526\t15.0\t274",
    "NA12879\tNA12886\t0.04779411764705882\t13.0\t272",
    "NA12879\tNA12887\t0.05947955390334572\t16.0\t269",
    "NA12879\tNA12888\t0.06666666666666667\t18.0\t270",
    "NA12879\tNA12889\t0.05514705882352941\t15.0\t272",
    "NA12879\tNA12890\t0.04428044280442804\t12.0\t271",
    "NA12879\tNA12891\t0.05223880597014925\t14.0\t268",
    "NA12879\tNA12892\t0.05166051660516605\t14.0\t271",
    "NA12879\tNA12893\t0.06642066420664207\t18.0\t271",
    "NA12880\tNA12881\t0.07063197026022305\t19.0\t269",
    "NA12880\tNA12882\t0.03731343283582089\t10.0\t268",
    "NA12880\tNA12883\t0.5475285171102662\t144.0\t263",
    "NA12880\tNA12884\t0.033707865168539325\t9.0\t267",
    "NA12880\tNA12885\t0.040892193308550186\t11.0\t269",
    "NA12880\tNA12886\t0.04119850187265917\t11.0\t267",
    "NA12880\tNA12887\t0.5584905660377358\t148.0\t265",
    "NA12880\tNA12888\t0.5526315789473685\t147.0\t266",
    "NA12880\tNA12889\t0.5430711610486891\t145.0\t267",
    "NA12880\tNA12890\t0.05947955390334572\t16.0\t269",
    "NA12880\tNA12891\t0.045627376425855515\t12.0\t263",
    "NA12880\tNA12892\t0.5373134328358209\t144.0\t268",
    "NA12880\tNA12893\t0.06015037593984962\t16.0\t266",
    "NA12881\tNA12882\t0.047619047619047616\t13.0\t273",
    "NA12881\tNA12883\t0.05303030303030303\t14.0\t264",
    "NA12881\tNA12884\t0.058823529411764705\t16.0\t272",
    "NA12881\tNA12885\t0.06569343065693431\t18.0\t274",
    "NA12881\tNA12886\t0.0625\t17.0\t272",
    "NA12881\tNA12887\t0.06691449814126393\t18.0\t269",
    "NA12881\tNA12888\t0.07037037037037037\t19.0\t270",
    "NA12881\tNA12889\t0.0661764705882353\t18.0\t272",
    "NA12881\tNA12890\t0.08118081180811808\t22.0\t271",
    "NA12881\tNA12891\t0.07462686567164178\t20.0\t268",
    "NA12881\tNA12892\t0.06642066420664207\t18.0\t271",
    "NA12881\tNA12893\t0.09225092250922509\t25.0\t271",
    "NA12882\tNA12883\t0.026615969581749048\t7.0\t263",
    "NA12882\tNA12884\t0.055350553505535055\t15.0\t271",
    "NA12882\tNA12885\t0.054945054945054944\t15.0\t273",
    "NA12882\tNA12886\t0.04428044280442804\t12.0\t271",
    "NA12882\tNA12887\t0.05970149253731343\t16.0\t268",
    "NA12882\tNA12888\t0.055762081784386616\t15.0\t269",
    "NA12882\tNA12889\t0.04059040590405904\t11.0\t271",
    "NA12882\tNA12890\t0.040740740740740744\t11.0\t270",
    "NA12882\tNA12891\t0.03731343283582089\t10.0\t268",
    "NA12882\tNA12892\t0.04814814814814815\t13.0\t270",
    "NA12882\tNA12893\t0.040740740740740744\t11.0\t270",
    "NA12883\tNA12884\t0.030303030303030304\t8.0\t264",
    "NA12883\tNA12885\t0.030303030303030304\t8.0\t264",
    "NA12883\tNA12886\t0.030303030303030304\t8.0\t264",
    "NA12883\tNA12887\t0.5576923076923077\t145.0\t260",
    "NA12883\tNA12888\t0.524904214559387\t137.0\t261",
    "NA12883\tNA12889\t0.5361216730038023\t141.0\t263",
    "NA12883\tNA12890\t0.04924242424242424\t13.0\t264",
    "NA12883\tNA12891\t0.03875968992248062\t10.0\t258",
    "NA12883\tNA12892\t0.5378787878787878\t142.0\t264",
    "NA12883\tNA12893\t0.05747126436781609\t15.0\t261",
    "NA12884\tNA12885\t0.05514705882352941\t15.0\t272",
    "NA12884\tNA12886\t0.051470588235294115\t14.0\t272",
    "NA12884\tNA12887\t0.0599250936329588\t16.0\t267",
    "NA12884\tNA12888\t0.048507462686567165\t13.0\t268",
    "NA12884\tNA12889\t0.037037037037037035\t10.0\t270",
    "NA12884\tNA12890\t0.03717472118959108\t10.0\t269",
    "NA12884\tNA12891\t0.03383458646616541\t9.0\t266",
    "NA12884\tNA12892\t0.040892193308550186\t11.0\t269",
    "NA12884\tNA12893\t0.048327137546468404\t13.0\t269",
    "NA12885\tNA12886\t0.04044117647058824\t11.0\t272",
    "NA12885\tNA12887\t0.05947955390334572\t16.0\t269",
    "NA12885\tNA12888\t0.05925925925925926\t16.0\t270",
    "NA12885\tNA12889\t0.051470588235294115\t14.0\t272",
    "NA12885\tNA12890\t0.04797047970479705\t13.0\t271",
    "NA12885\tNA12891\t0.041044776119402986\t11.0\t268",
    "NA12885\tNA12892\t0.055350553505535055\t15.0\t271",
    "NA12885\tNA12893\t0.06273062730627306\t17.0\t271",
    "NA12886\tNA12887\t0.056179775280898875\t15.0\t267",
    "NA12886\tNA12888\t0.04477611940298507\t12.0\t268",
    "NA12886\tNA12889\t0.04814814814814815\t13.0\t270",
    "NA12886\tNA12890\t0.048327137546468404\t13.0\t269",
    "NA12886\tNA12891\t0.04887218045112782\t13.0\t266",
    "NA12886\tNA12892\t0.03345724907063197\t9.0\t269",
    "NA12886\tNA12893\t0.055762081784386616\t15.0\t269",
    "NA12887\tNA12888\t0.5676691729323309\t151.0\t266",
    "NA12887\tNA12889\t0.5543071161048689\t148.0\t267",
    "NA12887\tNA12890\t0.05639097744360902\t15.0\t266",
    "NA12887\tNA12891\t0.06015037593984962\t16.0\t266",
    "NA12887\tNA12892\t0.5805243445692884\t155.0\t267",
    "NA12887\tNA12893\t0.06319702602230483\t17.0\t269",
    "NA12888\tNA12889\t0.5746268656716418\t154.0\t268",
    "NA12888\tNA12890\t0.0599250936329588\t16.0\t267",
    "NA12888\tNA12891\t0.045454545454545456\t12.0\t264",
    "NA12888\tNA12892\t0.5634328358208955\t151.0\t268",
    "NA12888\tNA12893\t0.0599250936329588\t16.0\t267",
    "NA12889\tNA12890\t0.05204460966542751\t14.0\t269",
    "NA12889\tNA12891\t0.04887218045112782\t13.0\t266",
    "NA12889\tNA12892\t0.5555555555555556\t150.0\t270",
    "NA12889\tNA12893\t0.05947955390334572\t16.0\t269",
    "NA12890\tNA12891\t0.04905660377358491\t13.0\t265",
    "NA12890\tNA12892\t0.05947955390334572\t16.0\t269",
    "NA12890\tNA12893\t0.0708955223880597\t19.0\t268",
    "NA12891\tNA12892\t0.045283018867924525\t12.0\t265",
    "NA12891\tNA12893\t0.06716417910447761\t18.0\t268",
    "NA12892\tNA12893\t0.06343283582089553\t17.0\t268",
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

  @Test
  public void testCloud() throws Exception {
    String[] ARGS = {
        "--project=" + helper.getTestProject(),
        "--references=" + helper.PLATINUM_GENOMES_BRCA1_REFERENCES,
        "--variantSetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--hasNonVariantSegments",
        "--output=" + outputPrefix,
        "--runner=BlockingDataflowPipelineRunner",
        "--stagingLocation=" + helper.getTestStagingGcsFolder()
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

