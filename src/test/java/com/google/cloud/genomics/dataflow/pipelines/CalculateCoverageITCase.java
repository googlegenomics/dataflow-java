/*
 * Copyright (C) 2017 Google Inc.
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

import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.*;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.Paginator;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;

/**
 * This integration test will use both the Genomics API and Cloud Storage.
 *
 * The following environment variables are required:
 * - a Google Cloud project name in TEST_PROJECT,
 *
 * When doing e.g. mvn install, you can skip integration tests using:
 *      mvn install -DskipITs
 *
 * To run one test:
 *      mvn -Dit.test=CalculateCoverageITCase#testAPI verify
 *
 * See also http://maven.apache.org/surefire/maven-failsafe-plugin/examples/single-test.html
 */
@RunWith(JUnit4.class)
public class CalculateCoverageITCase {

  // This file contains mother, father, and children of CEPH pedigree 1463. The variants of
  // the grandparents are retained.
  static final String BAM_PREFIX_FILEPATH = "src/test/resources/com/google/cloud/genomics/dataflow/pipelines/bamlist.txt";

  static IntegrationTestHelper helper;
  static Genomics genomics;
  Dataset dataset;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    helper = new IntegrationTestHelper();
    genomics = GenomicsFactory.builder().build().fromOfflineAuth(new OfflineAuth());
  }

  @Before
  public void setUp() throws Exception {
    Dataset datasetPrototype = new Dataset();
    datasetPrototype.setProjectId(helper.getTestProject());
    datasetPrototype.setName("CalculateCoverageITCase");
    Genomics.Datasets.Create dsRequest = genomics.datasets().create(datasetPrototype);
    dataset = dsRequest.execute();
  }

  @After
  public void tearDown() throws Exception {
    genomics.datasets().delete(dataset.getId()).execute();
  }

  @Test
  public void testAPI() throws Exception {
    String[] ARGS = {
        "--references=chr1:552960:553984",
        "--bucketWidth=1024",
        "--numQuantiles=6",
        "--outputDatasetId=" + dataset.getId(),
        "--inputDatasetId=" + helper.PLATINUM_GENOMES_DATASET,
        "--annotationSetName='Test API'",
        };
    testBase(ARGS, 1);
  }

  @Test
  public void testBAMPrefix() throws Exception {
    String[] ARGS = {
        "--references=chr1:552960:553984",
        "--bucketWidth=1024",
        "--numQuantiles=6",
        "--outputDatasetId=" + dataset.getId(),
        "--bamInput=gs://genomics-public-data/platinum-genomes/bam/",
        "--referenceSetId=CNfS6aHAoved2AEQ6PnzkOzw15rqAQ",
        "--annotationSetName='Test BAMs'",
    };
    testBase(ARGS, 1);
  }

  @Test
  public void testBAMGlob() throws Exception {
    String[] ARGS = {
        "--references=chr1:552960:553984",
        "--bucketWidth=1024",
        "--numQuantiles=6",
        "--outputDatasetId=" + dataset.getId(),
        "--bamInput=gs://genomics-public-data/platinum-genomes/bam/NA1287*",
        "--referenceSetId=CNfS6aHAoved2AEQ6PnzkOzw15rqAQ",
        "--annotationSetName='Test BAMs'",
    };
    testBase(ARGS, 1);
  }

  @Test
  public void testBAMList() throws Exception {
    String[] ARGS = {
        "--references=chr1:552960:553984",
        "--bucketWidth=1024",
        "--numQuantiles=6",
        "--outputDatasetId=" + dataset.getId(),
        "--bamInput=" + BAM_PREFIX_FILEPATH,
        "--referenceSetId=CNfS6aHAoved2AEQ6PnzkOzw15rqAQ",
        "--annotationSetName='Test file of BAMs'",
    };
    testBase(ARGS, 1);
  }

  @Test
  public void testBAMsWithMismatchedReferenceSet() throws Exception {
    // The default reference set id is not the correct one to use with Platinum Genomes.
    // This pipeline run will not fail but it will result in no annotations created
    // due to this mismatch.
    String[] ARGS = {
        "--references=chr1:552960:553984",
        "--bucketWidth=1024",
        "--numQuantiles=6",
        "--outputDatasetId=" + dataset.getId(),
        "--bamInput=gs://genomics-public-data/platinum-genomes/bam/",
        "--annotationSetName='Test mismatched reference set id for BAMs'",
    };
    testBase(ARGS, 0);
  }

  private void testBase(String[] ARGS, int expectedResult) throws Exception {
    // Run the pipeline.
    CalculateCoverage.main(ARGS);

    // Confirm that annotations were created.
    Iterable<AnnotationSet> annotationSets = Paginator.AnnotationSets.create(genomics)
        .search(new SearchAnnotationSetsRequest().setDatasetIds(Lists.newArrayList(dataset.getId())),
            "annotationSets(id),nextPageToken");
    int numAnnotationSets = 0;
    int numAnnotations = 0;
    for (AnnotationSet annotationSet : annotationSets) {
      numAnnotationSets++;
      Iterable<Annotation> annotations = Paginator.Annotations.create(genomics, ShardBoundary.Requirement.OVERLAPS)
          .search(new SearchAnnotationsRequest().setAnnotationSetIds(Lists.newArrayList(annotationSet.getId())));
      for (Annotation annotation : annotations) {
        numAnnotations++;
        System.out.println(annotation.toPrettyString());
      }
    }
    assertEquals(1, numAnnotationSets);
    assertEquals(expectedResult, numAnnotations);
  }
}
