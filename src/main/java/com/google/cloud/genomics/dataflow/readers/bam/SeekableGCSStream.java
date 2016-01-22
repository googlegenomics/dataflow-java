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
package com.google.cloud.genomics.dataflow.readers.bam;

import com.google.api.client.http.HttpResponse;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects.Get;
import com.google.api.services.storage.model.StorageObject;

import htsjdk.samtools.seekablestream.SeekableStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Adapter of GCS to look like HTSJDK SeekableStream so BAM readers
 * can stream data from GCS.
 * TODO: implement more sophisticated retries with backoff.
 */
public class SeekableGCSStream extends SeekableStream {
  private static final Logger LOG = Logger.getLogger(SeekableGCSStream.class.getName());
  
  private static final Pattern SLASH = Pattern.compile("/");
  private static final String GCS_PREFIX = "gs://";
  private static final int MAX_RETRIES = 3;
  private static final long RETRY_SLEEP_TIME_MSEC = 1000;
  
  private Storage.Objects client;
  private StorageObject object;
  private String name;
  private Get get;
  private InputStream stream = null;
  private long size = -1;
  private long position = -1;
  private byte[] oneByte = new byte[1];
  private boolean atEof = false;
  
  public SeekableGCSStream(Storage.Objects client, String name) throws IOException {
    LOG.info("Creating SeekableGCSStream: " + name);
    this.client = client;
    object = uriToStorageObject(name);
    get = this.client.get(object.getBucket(), object.getName());
    seek(0);
  }

  @Override
  public void close() throws IOException {
    if (stream != null) {
      stream.close();
      stream = null;
    }
  }

  @Override
  public boolean eof() throws IOException {
    return atEof;
  }

  @Override
  public String getSource() {
    return name;
  }

  @Override
  public long length() {
    return size;
  }

  @Override
  public long position() throws IOException {
    return position;
  }
  
  @Override
  public int read() throws IOException {
    final int bytesRead = read(oneByte, 0, 1);
    return bytesRead==1 ? oneByte[0] : -1;
  }

  @Override
  public void seek(long arg0) throws IOException {
    if (arg0 == position) {
      return;
    }
    validatePosition(arg0);
    position = arg0;
    openStream();
  }
  
  protected void openStream()
      throws IOException {
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.finest("openStream: " + position);
    }
    try {
      close();
    } catch (Exception ex) {
      // Ignore problems in closing the old stream
    }
    validatePosition(position);
    get.getRequestHeaders()
        .setRange(String.format("bytes=%d-", position));
    HttpResponse response;
    try {
      response = get.executeMedia();
    } catch (IOException e) {
      String msg = String.format("Error reading %s at position %d",
          name, position);
      atEof = true;
      throw new IOException(msg, e);
    }
    String contentRange = response.getHeaders().getContentRange();
    if (response.getHeaders().getContentLength() != null) {
      size = response.getHeaders().getContentLength() + position;
    } else if (contentRange != null) {
      String sizeStr = SLASH.split(contentRange)[1];
      try {
        size = Long.parseLong(sizeStr);
      } catch (NumberFormatException e) {
        throw new IOException(
            "Could not determine size from response from Content-Range: " + contentRange, e);
      }
    } else {
      throw new IOException("Could not determine size of response");
    }
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.finest("stream size=" + size);
    }
    atEof = size == 0;
    stream = response.getContent();
  }
  
  protected void validatePosition(long newPosition) {
    if (newPosition < 0) {
      throw new IllegalArgumentException(
          String.format("Invalid seek offset: position value (%d) must be >= 0", newPosition));
    }

    if ((size >= 0) && (newPosition >= size)) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid seek offset: position value (%d) must be between 0 and %d",
              newPosition, size));
    }
  }
  
  @Override
  public int read(byte[] buf, int offset, int len)
      throws IOException {
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.finest("Read at offset " + offset + " length " + len);
    }
    if (len == 0 || offset >= buf.length) {
      return 0;
    }

    int totalBytesRead = 0;
    int retriesAttempted = 0;

    do {
      try {
        final int numBytesRead = stream.read(buf, 
            offset + totalBytesRead, len - totalBytesRead);
        if (LOG.isLoggable(Level.FINEST)) {
          LOG.finest("Read returned: " + numBytesRead);
        }
        if (numBytesRead < 0) {
          atEof = true;
          break;
        }
        totalBytesRead += numBytesRead;
        position += numBytesRead;

        retriesAttempted = 0;
      } catch (IOException ioe) {
        if (retriesAttempted == MAX_RETRIES) {
          LOG.warning(
              String.format("Already attempted max of %d retries while reading '%s'; throwing exception.",
              retriesAttempted, name));
          throw ioe;
        } else {
          ++retriesAttempted;
          LOG.warning(String.format("Got exception: %s while reading '%s'; retry # %d. Sleeping...",
              ioe.getMessage(), name, retriesAttempted));

          try {
            // TODO: implement backoff logic instead of simplistic static sleep interval
            Thread.sleep(RETRY_SLEEP_TIME_MSEC);
          } catch (InterruptedException ie) {
            LOG.warning(String.format("Interrupted while sleeping before retry. Giving up "
                + "after %d retries for '%s'", retriesAttempted, name));
            ioe.addSuppressed(ie);
            throw ioe;
          }
          LOG.info(String.format("Done sleeping before retry for '%s'; retry # %d.",
              name, retriesAttempted));

          openStream();
        }
      }
    } while (totalBytesRead < len);

    final int retVal = (totalBytesRead == 0) ? -1 : totalBytesRead;
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.finest("Read result " + retVal);
    }
    return retVal;
  }
  
  private static StorageObject uriToStorageObject(String uri) throws IOException {
    StorageObject object = new StorageObject();
    if (uri.startsWith(GCS_PREFIX)) {
      uri = uri.substring(GCS_PREFIX.length());
    } else {
      throw new IOException("Invalid GCS path (does not start with gs://): " + uri);
    }
    int slashPos = uri.indexOf("/");
    if (slashPos > 0) {
      object.setBucket(uri.substring(0, slashPos));
      object.setName(uri.substring(slashPos + 1));
      LOG.info("uriToStorageObject " + uri + "=" + object.getBucket() + ":" + object.getName());
    } else {
      throw new IOException("Invalid GCS path (does not have bucket/name form): " + uri);
    }
    return object;
  }
}