/*
 * Copyright (C) 2014 Google Inc.
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
package com.google.cloud.genomics.dataflow.utils;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import com.google.cloud.genomics.utils.OfflineAuth;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 *
 * This test expects you to have a Google Cloud API key in the GOOGLE_API_KEY environment variable.
 * It will read from GCS, so it's not your usual unit test (hence the integration test-related name).
 *
 */
@RunWith(JUnit4.class)
public class GCSHelperITCase {

  // this file shouldn't move.
  final String TEST_BUCKET = "genomics-public-data";
  final String TEST_FNAME = "ftp-trace.ncbi.nih.gov/1000genomes/ftp/20131219.populations.tsv";
  final long   TEST_FSIZE = 1663;
  final byte[] TEST_MD5 = new byte[] {-6, 94, 5, 25, 38, 68, 74, -127, -13, 94, -72, 7, -53, 111, 99, -3};

  // Test the various ways of getting a GCSHelper

  @Test
  public void testOfflineAuth() throws Exception {
    GenomicsOptions options = PipelineOptionsFactory.as(GenomicsOptions.class);
    OfflineAuth offlineAuth = GenomicsOptions.Methods.getGenomicsAuth(options);
    GCSHelper gcsHelper = new GCSHelper(offlineAuth);
    long fileSize = gcsHelper.getFileSize(TEST_BUCKET, TEST_FNAME);
    Assert.assertEquals(TEST_FSIZE, fileSize);
  }

  @Test
  public void testPipelineOptions() throws Exception {
    GenomicsOptions options = PipelineOptionsFactory.as(GenomicsOptions.class);
    GCSHelper gcsHelper = new GCSHelper(options);
    long fileSize = gcsHelper.getFileSize(TEST_BUCKET, TEST_FNAME);
    Assert.assertEquals(TEST_FSIZE, fileSize);
  }

  // Test the GCSHelper's methods

  @Test
  public void testGetPartial() throws Exception {
    GenomicsOptions options = PipelineOptionsFactory.as(GenomicsOptions.class);
    GCSHelper gcsHelper = new GCSHelper(options);
    String partial = gcsHelper.getPartialObjectData(TEST_BUCKET, TEST_FNAME, 34, 37).toString();
    Assert.assertEquals("Code", partial);
  }

  @Test
  public void testGetWhole() throws Exception {
    GenomicsOptions options = PipelineOptionsFactory.as(GenomicsOptions.class);
    GCSHelper gcsHelper = new GCSHelper(options);
    InputStream input = gcsHelper.getWholeObject(TEST_BUCKET, TEST_FNAME);
    byte[] digest = md5sum(input);
    Assert.assertTrue("file MD5 doesn't match.", Arrays.equals(TEST_MD5, digest));
  }

  @Test
  public void testDownload() throws Exception {
    GenomicsOptions options = PipelineOptionsFactory.as(GenomicsOptions.class);
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
