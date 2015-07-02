package com.google.cloud.genomics.dataflow.pipelines;

import com.google.api.services.storage.Storage;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.security.GeneralSecurityException;

/**
 * This test expects you to have:
 * -a Google Cloud API key in the GOOGLE_API_KEY environment variable,
 * -your Google Cloud project name in TEST_PROJECT,
 * -a GCS folder path in TEST_OUTPUT_GCS_FOLDER to store temporary test outputs,
 * -a GCS folder path in TEST_STAGING_GCS_FOLDER to store temporary files,
 * GCS folder paths should be of the form "gs://bucket/folder/"
 *
 * This test will read and write to GCS, and call the Genomics API.
 *
 * When doing e.g. mvn install, you can skip integration tests using:
 * mvn install -DskipITs
 */
@RunWith(JUnit4.class)
public class CountReadsITCase {

  final String API_KEY = System.getenv("GOOGLE_API_KEY");
  final String TEST_PROJECT = System.getenv("TEST_PROJECT");
  final String TEST_OUTPUT_GCS_FOLDER = System.getenv("TEST_OUTPUT_GCS_FOLDER");
  final String TEST_STAGING_GCS_FOLDER = System.getenv("TEST_STAGING_GCS_FOLDER");
  // This file shouldn't move.
  final String TEST_BAM_FNAME = "gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/pilot_data/data/NA06985/alignment/NA06985.454.MOSAIK.SRP000033.2009_11.bam";
  // This is the Readgroupset ID of the same file, in ReadStore. It also shouldn't move.
  final String TEST_READGROUPSET = "CMvnhpKTFhDvp9zAvYj66AY";
  // The region where we're counting reads.
  final String TEST_CONTIG = "1:550000:560000";
  // How many reads are in that region.
  final long TEST_EXPECTED = 685;
  // In this file there are no unmapped reads, so expecting the same number.
  final long TEST_EXPECTED_WITH_UNMAPPED = TEST_EXPECTED;
  
  // Same as the above variables, but for the NA12877_S1 dataset.
  final String NA12877_S1_BAM_FILENAME = "gs://genomics-public-data/platinum-genomes/bam/NA12877_S1.bam";
  final String NA12877_S1_READGROUPSET = "CMvnhpKTFhD3he72j4KZuyc";
  final String NA12877_S1_CONTIG = "chr17:41196311:41277499";
  final long NA12877_S1_EXPECTED = 45081;
  // How many reads are in that region if we take unmapped ones too    
  final long NA12877_S1_EXPECTED_WITH_UNMAPPED = 45142;

  @Before
  public void voidEnsureEnvVar() {
    Assert.assertNotNull("You must set the GOOGLE_API_KEY environment variable for this test.", API_KEY);
    Assert.assertNotNull("You must set the TEST_PROJECT environment variable for this test.", TEST_PROJECT);
    Assert.assertNotNull("You must set the TEST_OUTPUT_GCS_FOLDER environment variable for this test.", TEST_OUTPUT_GCS_FOLDER);
    Assert.assertTrue("TEST_OUTPUT_GCS_FOLDER must end with '/'", TEST_OUTPUT_GCS_FOLDER.endsWith("/"));
    Assert.assertTrue("TEST_OUTPUT_GCS_FOLDER must start with 'gs://'", TEST_OUTPUT_GCS_FOLDER.startsWith("gs://"));
    Assert.assertNotNull("You must set the TEST_STAGING_GCS_FOLDER environment variable for this test.", TEST_STAGING_GCS_FOLDER);
    Assert.assertTrue("TEST_STAGING_GCS_FOLDER must start with 'gs://'", TEST_STAGING_GCS_FOLDER.startsWith("gs://"));
    // we don't care how TEST_STAGING_GCS_FOLDER ends, so no check for it.
  }
  
  private void testLocalBase(String outputFilename, String contig, String bamFilename, long expectedCount,
      boolean includeUnmapped) throws Exception {
    final String OUTPUT = TEST_OUTPUT_GCS_FOLDER + outputFilename;
    String[] ARGS = {
        "--apiKey=" + API_KEY,
        "--project=" + TEST_PROJECT,
        "--output=" + OUTPUT,
        "--references=" + contig,
        "--includeUnmapped=" + includeUnmapped,
        "--BAMFilePath=" + bamFilename,
    };
    GenomicsOptions popts = PipelineOptionsFactory.create().as(GenomicsOptions.class);
    popts.setApiKey(API_KEY);
    GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(popts);
    try {
      touchOutput(gcsUtil, OUTPUT);

      CountReads.main(ARGS);

      BufferedReader reader = new BufferedReader(Channels.newReader(gcsUtil.open(GcsPath.fromUri(OUTPUT)), "UTF-8"));
      long got = Long.parseLong(reader.readLine());

    Assert.assertEquals(expectedCount, got);
    } finally {
      GcsDelete(popts, OUTPUT);
    }
  }

  /**
   * CountReads running on the client's machine.
   */
  @Test
  public void testLocal() throws Exception {
    testLocalBase("CountReadsITCase-testLocal-output.txt",
        TEST_CONTIG, TEST_BAM_FNAME, TEST_EXPECTED, false);
  }
  
  @Test
  public void testLocalUnmapped() throws Exception {
    testLocalBase("CountReadsITCase-testLocal-output.txt",
        TEST_CONTIG, TEST_BAM_FNAME, TEST_EXPECTED_WITH_UNMAPPED, true);
  }
  
  @Test
  public void testLocalNA12877_S1() throws Exception {
    testLocalBase("CountReadsITCase-testLocal-NA12877_S1-output.txt",
        NA12877_S1_CONTIG, NA12877_S1_BAM_FILENAME, NA12877_S1_EXPECTED, false);
  }
  
  @Test
  public void testLocalNA12877_S1_UNMAPPED() throws Exception {
    testLocalBase("CountReadsITCase-testLocal-NA12877_S1-output.txt",
        NA12877_S1_CONTIG, NA12877_S1_BAM_FILENAME, 
        NA12877_S1_EXPECTED_WITH_UNMAPPED, true);
  }

  private void testCloudBase(String outputFilename, String contig, String bamFilename, long expectedCount) throws Exception {
    final String OUTPUT = TEST_OUTPUT_GCS_FOLDER + outputFilename;
    String[] ARGS = {
        "--apiKey=" + API_KEY,
        "--project=" + TEST_PROJECT,
        "--output=" + OUTPUT,
        "--numWorkers=2",
        "--runner=BlockingDataflowPipelineRunner",
        "--stagingLocation=" + TEST_STAGING_GCS_FOLDER,
        "--references=" + contig,
        "--BAMFilePath=" + bamFilename
    };
    GenomicsOptions popts = PipelineOptionsFactory.create().as(GenomicsOptions.class);
    popts.setApiKey(API_KEY);
    GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(popts);
    try {
      touchOutput(gcsUtil, OUTPUT);

      CountReads.main(ARGS);

      BufferedReader reader = new BufferedReader(Channels.newReader(gcsUtil.open(GcsPath.fromUri(OUTPUT)), "UTF-8"));
      long got = Long.parseLong(reader.readLine());

      Assert.assertEquals(expectedCount, got);
    } finally {
      GcsDelete(popts, OUTPUT);
    }
  }
  
  /**
   * CountReads running on Dataflow.
   */
  @Test
  public void testCloud() throws Exception {
    testCloudBase("CountReadsITCase-testCloud-output.txt",
        TEST_CONTIG, TEST_BAM_FNAME, TEST_EXPECTED);
  }
  
  @Test
  public void testCloudNA12877_S1() throws Exception {
    testCloudBase("CountReadsITCase-testCloud-NA12877_S1-output.txt",
        NA12877_S1_CONTIG, NA12877_S1_BAM_FILENAME, NA12877_S1_EXPECTED);
  }

  public void testCloudWithAPIBase(String outputFilename, String contig, String readGroupSetId, long expectedCount) throws Exception {
    final String OUTPUT = TEST_OUTPUT_GCS_FOLDER + outputFilename;
    String[] ARGS = {
        "--apiKey=" + API_KEY,
        "--project=" + TEST_PROJECT,
        "--output=" + OUTPUT,
        "--numWorkers=2",
        "--runner=BlockingDataflowPipelineRunner",
        "--stagingLocation=" + TEST_STAGING_GCS_FOLDER,
        "--references=" + contig,
        "--readGroupSetId=" + readGroupSetId
    };
    GenomicsOptions popts = PipelineOptionsFactory.create().as(GenomicsOptions.class);
    popts.setApiKey(API_KEY);
    GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(popts);
    try {
      touchOutput(gcsUtil, OUTPUT);

      CountReads.main(ARGS);

      BufferedReader reader = new BufferedReader(Channels.newReader(gcsUtil.open(GcsPath.fromUri(OUTPUT)), "UTF-8"));
      long got = Long.parseLong(reader.readLine());

      Assert.assertEquals(expectedCount, got);
    } finally {
      GcsDelete(popts, OUTPUT);
    }
  }

  /**
   * CountReads running on Dataflow with API input.
   */
  @Test
  public void testCloudWithAPI() throws Exception {
    testCloudWithAPIBase("CountReadsITCase-testCloudWithAPI-output.txt",
        TEST_CONTIG, TEST_READGROUPSET, TEST_EXPECTED);
  }

  @Test
  public void testCloudWithAPI_NA12877_S1() throws Exception {
    testCloudWithAPIBase("CountReadsITCase-testCloudWithAPI-NA12877_S1-output.txt",
        NA12877_S1_CONTIG, NA12877_S1_READGROUPSET, NA12877_S1_EXPECTED);
  }

  /**
   * Make sure we can get to the output.
   */
  private void touchOutput(GcsUtil gcsUtil, String outputGcsPath) throws IOException {
    try (Writer writer = Channels.newWriter(gcsUtil.create(GcsPath.fromUri(outputGcsPath), "text/plain"), "UTF-8")) {
      writer.write("output will go here");
    }
  }

  private static void GcsDelete(PipelineOptions popts, String gcsPath) throws IOException, GeneralSecurityException {
    // boilerplate
    GcsPath path = GcsPath.fromUri(gcsPath);
    GcsOptions gcsOptions = (GcsOptions)popts.as(GcsOptions.class);
    Storage storage = Transport.newStorageClient(gcsOptions).build();
    // do the actual work
    storage.objects().delete(path.getBucket(), path.getObject()).execute();
  }

}