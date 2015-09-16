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
package com.google.cloud.genomics.dataflow.utils;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * FilterOutputStream that writes all but the last bytesToTruncate bytes to
 * the underlying OutputStream.
 */
public class TruncatedOutputStream extends FilterOutputStream {
  private byte[] buf;
  private int count;
  private int bytesToTruncate;
  OutputStream os;
  
  public TruncatedOutputStream(OutputStream os, int bytesToTruncate) {
    super(os);
    this.os = os;
    this.buf = new byte[ Math.max(1024, bytesToTruncate) ];
    this.count = 0;
    this.bytesToTruncate = bytesToTruncate;
  }
  
  @Override
  public void write(int b) throws IOException {
    if (count == buf.length) {
      flushBuffer();
    }
    buf[count++] = (byte)b;
  }
  
  @Override
  public void write(byte[] data) throws IOException {
    write(data, 0, data.length);
  }
  
  @Override
  public void write(byte[] data, int offset, int length) throws IOException {
    flushBuffer();
    int spaceRemaining = buf.length - count;
    if (length < spaceRemaining) {
      System.arraycopy(data, offset, buf, count, length);
      count += length;
    } else if (length >= bytesToTruncate) {
      // We have more than bytesToTruncate to write, so clear the buffer
      // completely, and write all but bytesToTruncate directly to the stream.
      os.write(buf, 0, count);
      final int bytesToWriteDrirectly = length - bytesToTruncate;
      os.write(data, offset, bytesToWriteDrirectly);
      System.arraycopy(data, offset + bytesToWriteDrirectly, buf, 0, bytesToTruncate);
      count = bytesToTruncate;
    } else {
      // Need this many of the current bytes to stay in the buffer to ensure we
      // have at least bytesToTruncate.
      final int keepInBuffer = bytesToTruncate - length;
      // Write the rest to the stream.
      final int bytesToDumpFromBuffer = count - keepInBuffer;
      os.write(buf, 0, bytesToDumpFromBuffer);
      System.arraycopy(buf, bytesToDumpFromBuffer, buf, 0, keepInBuffer);
      System.arraycopy(data, offset, buf, keepInBuffer, length);
      count = bytesToTruncate;
    }
  }
  
  @Override
  public synchronized void flush() throws IOException {
    flushBuffer();
    os.flush();
  }
  
  @Override
  public void close() throws IOException {
    flushBuffer();
    os.close();
  }
  
  private void flushBuffer() throws IOException {
    final int bytesWeCanSafelyWrite = count - bytesToTruncate;
    if (count > bytesToTruncate) {
      os.write(buf, 0, bytesWeCanSafelyWrite);
      System.arraycopy(buf, bytesWeCanSafelyWrite, buf, 0, bytesToTruncate);
      count = bytesToTruncate;
    }
  }
}
