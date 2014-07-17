package com.google.cloud.genomics.dataflow.utils;

import com.google.cloud.dataflow.sdk.util.GcsUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Utilities for working with GCS files
 */
public class GcsFileUtil {
  
  /**
   * Given a path to a local file and desired gcs path, copies the local file to GCS
   */
  public static void localToGcs(String localFilePath, String gcsFilePath,
      GcsUtil gcsUtil, String fileType, long bufferSize) throws IOException {
    RandomAccessFile localFile = new RandomAccessFile(localFilePath, "r");
    FileChannel localChannel = localFile.getChannel();
    WritableByteChannel gcsChannel = gcsUtil.create(
        GcsUtil.asGcsFilename(gcsFilePath), fileType);
    
    long pos = 0;
    while (localChannel.transferTo(pos, bufferSize, gcsChannel) == bufferSize) {
      pos += bufferSize;
    }
    gcsChannel.close();
    localChannel.close();
    localFile.close();
  }
  
  /**
   * Given a path to a gcs file and a local path, copies the gcs file to local path
   */
  public static void gcsToLocal(String gcsFilePath, String localFilePath,
      GcsUtil gcsUtil, long bufferSize) throws IOException {
    RandomAccessFile localFile = new RandomAccessFile(localFilePath, "w");
    FileChannel localChannel = localFile.getChannel();
    SeekableByteChannel gcsChannel = gcsUtil.open(GcsUtil.asGcsFilename(gcsFilePath));
    
    long pos = 0;
    while (localChannel.transferFrom(gcsChannel, pos, bufferSize) == bufferSize) {
      pos += bufferSize;
    }
    gcsChannel.close();
    localChannel.close();
    localFile.close();
  }
}
