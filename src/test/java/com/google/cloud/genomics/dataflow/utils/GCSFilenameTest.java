package com.google.cloud.genomics.dataflow.utils;

import static org.junit.Assert.assertEquals;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GCSFilenameTest  {

  @Test
  public void testGetGCSPathRoundtrip() throws Exception {
    String[] urls = {
        "gs://bucket/path/file.xml",
        "gs://bucket/file"
    };
    for (String url : urls) {
      String roundTrip = new GCSFilename(url).getGCSPath();
      assertEquals(url, roundTrip);
    }
  }

  @Test
  public void testGetGCSPathMalformed() throws Exception {
    String[] urls = {
        "C:\\AUTOEXEC.BAT",
        "file://foo/bar",
        "foo/bar",
        "http://example.com",
        "gs://bucket"
    };
    for (String url : urls) {
      try {
        String roundTrip = new GCSFilename(url).getGCSPath();
        Assert.fail("Should have gotten an error for invalid GCS URL '" + url + "'");
      } catch (Exception x) {
        // Good, we wanted an exception
      }

    }
  }
}