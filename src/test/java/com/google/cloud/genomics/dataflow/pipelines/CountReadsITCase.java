package com.google.cloud.genomics.dataflow.pipelines;

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.BufferedReader;
import java.nio.channels.Channels;

/**
 * This test expects you to have a Google Cloud API key in the GOOGLE_API_KEY environment variable,
 * and a GCS folder path in TEST_OUTPUT_GCS_FOLDER to store temporary test outputs.
 * It should be of the form "gs://bucket/folder/"
 *
 * This test will read and write to GCS.
 */
@RunWith(JUnit4.class)
public class CountReadsITCase {

  final String API_KEY = System.getenv("GOOGLE_API_KEY");
  final String TEST_OUTPUT_GCS_FOLDER = System.getenv("TEST_OUTPUT_GCS_FOLDER");
  // this file shouldn't move.
  final String TEST_BAM_FNAME = "gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/pilot_data/data/NA06985/alignment/NA06985.454.MOSAIK.SRP000033.2009_11.bam";

  @Before
  public void voidEnsureEnvVar() {
    Assert.assertNotNull("You must set the GOOGLE_API_KEY environment variable for this test.", API_KEY);
    Assert.assertNotNull("You must set the TEST_OUTPUT_GCS_FOLDER environment variable for this test.", TEST_OUTPUT_GCS_FOLDER);
    Assert.assertTrue("TEST_OUTPUT_GCS_FOLDER must end with '/'", TEST_OUTPUT_GCS_FOLDER.endsWith("/"));
    Assert.assertTrue("TEST_OUTPUT_GCS_FOLDER must start with 'gs://'", TEST_OUTPUT_GCS_FOLDER.startsWith("gs://"));
  }

  /**
   * CountReads running on the client's machine.
   */
  @Test
  public void testLocal() throws Exception {
    final String OUTPUT = TEST_OUTPUT_GCS_FOLDER + "CountReadsITCase-testLocal-output.txt";
    String[] ARGS = {
        "--apiKey="+API_KEY,
        "--project=genomics-pipelines",
        "--output=" + OUTPUT,
        "--references=1:550000:560000",
        "--BAMFilePath="+TEST_BAM_FNAME
    };
    final long EXPECTED = 685;
    GenomicsOptions popts = PipelineOptionsFactory.create().as(GenomicsOptions.class);
    popts.setApiKey(API_KEY);
    GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(popts);

    CountReads.main(ARGS);

    BufferedReader reader = new BufferedReader(Channels.newReader(gcsUtil.open(GcsPath.fromUri(OUTPUT)), "UTF-8"));
    long got = Long.parseLong(reader.readLine());
    Assert.assertEquals(EXPECTED, got);
  }

}