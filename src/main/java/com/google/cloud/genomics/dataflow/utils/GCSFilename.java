package com.google.cloud.genomics.dataflow.utils;

import java.io.IOException;

/**
 * A holder for the (bucket, filename) pairs we get with Google Cloud Storage,
 * plus helper code to translate between the (bucket,filename) and "gs://bucket/filename" formats.
 */
public class GCSFilename {
  public static final String PREFIX = "gs://";
  public final String bucket;
  public final String filename;

  /**
   * Hold the (bucket,filename) pair.
   * filename can include "/", bucket cannot.
   */
  public GCSFilename(String bucket, String filename) {
    this.bucket = bucket;
    this.filename = filename;
  }

  /**
   * Parse a GCD URL and hold the result.
   * gcsPathUrl is of the form gs://BUCKET/FILENAME. filename can include "/", bucket cannot.
   * @throws IOException
   */
  public GCSFilename(String gcsPathUrl) throws IOException {
    if (!gcsPathUrl.startsWith(PREFIX)) {
      throw new IOException("Invalid GCS URL (does not start with " + PREFIX + "): " + gcsPathUrl);
    }
    String suffix = gcsPathUrl.substring(PREFIX.length());
    int slashPos = suffix.indexOf("/");
    if (slashPos < 0) {
      throw new IOException("Invalid GCS URL (does not contain a '/'): " + gcsPathUrl);
    }
    this.bucket = suffix.substring(0, slashPos);
    this.filename = suffix.substring(slashPos + 1);
  }

  /**
   * gs://bucket/filename
   */
  public String getGCSPath() {
    return PREFIX + bucket + "/" + filename;
  }
}