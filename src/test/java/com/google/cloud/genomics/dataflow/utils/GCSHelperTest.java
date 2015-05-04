package com.google.cloud.genomics.dataflow.utils;


import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.common.hash.HashingInputStream;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 *
 * This test requires ../client-secrets.json to be present.
 *
 */
@RunWith(JUnit4.class)
public class GCSHelperTest {

  final String APP = "GCSHelperTest";
  final String[] ARGS = { "--genomicsSecretsFile=../client-secrets.json" };
  // gs://dataflow-samples/shakespeare/kinglear.txt is part of the dataflow documentation and shouldn't move.
  final String TEST_BUCKET = "dataflow-samples";
  final String TEST_FNAME = "shakespeare/kinglear.txt";
  final long   TEST_FSIZE = 157283;
  final byte[] TEST_MD5 = new byte[] {-33,-61,-91,12,44,48,-29,104,-86,62,72,-117,37,-52,81,14};

  // Test the various ways of getting a GCSHelper

  @Test
  public void testClientSecrets() throws Exception {
    GCSHelper gcsHelper = new GCSHelper(APP, "../client-secrets.json");
    long fileSize = gcsHelper.getFileSize(TEST_BUCKET, TEST_FNAME);
    // the test file shouldn't be changing
    Assert.assertEquals(TEST_FSIZE, fileSize);
  }

  @Test
  public void testOfflineAuth() throws Exception {
    GenomicsOptions options = PipelineOptionsFactory.fromArgs(ARGS).as(GenomicsOptions.class);
    GenomicsFactory.OfflineAuth offlineAuth = GenomicsOptions.Methods.getGenomicsAuth(options);
    GCSHelper gcsHelper = new GCSHelper(APP, offlineAuth);
    long fileSize = gcsHelper.getFileSize(TEST_BUCKET, TEST_FNAME);
    Assert.assertEquals(TEST_FSIZE, fileSize);
  }

  @Test
  public void testPipelineOptions() throws Exception {
    GenomicsOptions options = PipelineOptionsFactory.fromArgs(ARGS).as(GenomicsOptions.class);
    GCSHelper gcsHelper = new GCSHelper(options);
    long fileSize = gcsHelper.getFileSize(TEST_BUCKET, TEST_FNAME);
    Assert.assertEquals(TEST_FSIZE, fileSize);
  }

  // Test the GCSHelper's methods

  @Test
  public void testGetPartial() throws Exception {
    GenomicsOptions options = PipelineOptionsFactory.fromArgs(ARGS).as(GenomicsOptions.class);
    GCSHelper gcsHelper = new GCSHelper(options);
    String lear = gcsHelper.getPartialObjectData(TEST_BUCKET, TEST_FNAME, 34, 37).toString();
    Assert.assertEquals("LEAR", lear);
  }

  @Test
  public void testGetWhole() throws Exception {
    GenomicsOptions options = PipelineOptionsFactory.fromArgs(ARGS).as(GenomicsOptions.class);
    GCSHelper gcsHelper = new GCSHelper(options);
    InputStream input = gcsHelper.getWholeObject(TEST_BUCKET, TEST_FNAME);
    byte[] digest = md5sum(input);
    Assert.assertTrue("file MD5 doesn't match.", Arrays.equals(TEST_MD5, digest));
  }

  @Test
  public void testDownload() throws Exception {
    GenomicsOptions options = PipelineOptionsFactory.fromArgs(ARGS).as(GenomicsOptions.class);
    GCSHelper gcsHelper = new GCSHelper(options);
    File tmpFile = gcsHelper.getAsFile(TEST_BUCKET, TEST_FNAME);
    byte[] digest = md5sum(new FileInputStream(tmpFile));
    Assert.assertTrue("file MD5 doesn't match.", Arrays.equals(TEST_MD5, digest));
  }


  private byte[] md5sum(InputStream input) throws NoSuchAlgorithmException, IOException {
    MessageDigest md = MessageDigest.getInstance("MD5");
    DigestInputStream dis = new DigestInputStream(input, md);
    byte[] buf = new byte[1024*1024];
    while (true) {
      int read = dis.read(buf);
      // EOF
      if (read<=0) break;
    }
    return md.digest();
  }

}
