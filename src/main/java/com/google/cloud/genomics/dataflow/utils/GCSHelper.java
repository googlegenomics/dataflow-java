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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.genomics.utils.CredentialFactory;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * A helper class to download from Google Cloud Storage.
 */
public class GCSHelper {

  private static final Logger LOGGER = Logger.getLogger(GCSHelper.class.getName());
  private static final boolean IS_APP_ENGINE = false;
  private Storage storage;

  /**
   * Global instance of the HTTP transport.
   */
  private static HttpTransport httpTransport;
  /**
   * Global instance of the JSON factory.
   */
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();


  /**
   * Connects to storage (this is the preferred way).
   *
   * @param popts already-filled options.
   */
  public GCSHelper(GenomicsOptions popts) throws GeneralSecurityException, IOException {
    Preconditions.checkNotNull(popts);
    // set up storage object
    GenomicsFactory factory = GenomicsFactory.builder(popts.getAppName())
        .build();
    httpTransport = factory.getHttpTransport();
    Storage.Builder builder = new Storage.Builder(httpTransport, JSON_FACTORY, null)
        .setApplicationName(popts.getAppName());
    OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(popts);
    storage = factory.fromOfflineAuth(builder, auth).build();
  }

  /**
   * Connects to storage
   * (use this if you're a Dataflow worker, as you don't have access to the clients-secrets.json from there).
   *
   * @param offlineAuth serialized credentials
   */
  public GCSHelper(OfflineAuth offlineAuth) throws GeneralSecurityException, IOException {
    Preconditions.checkNotNull(offlineAuth);
    // set up storage object
    GenomicsFactory factory = GenomicsFactory.builder().build();
    httpTransport = factory.getHttpTransport();
    Storage.Builder builder = new Storage.Builder(httpTransport, JSON_FACTORY, null);
    storage = factory.fromOfflineAuth(builder, offlineAuth).build();
  }

  /**
   * Connects to storage.
   *
   * @param appName     name of your app
   * @param secretsFile path to clients-secrets.json
   */
  public GCSHelper(String appName, String secretsFile) throws GeneralSecurityException, IOException {
    // cf https://groups.google.com/forum/#!msg/google-genomics-discuss/P9A9odUXwaM/ISdIzOXNS3YJ
    GenomicsFactory factory = GenomicsFactory.builder(appName).build();
    httpTransport = factory.getHttpTransport();
    Credential creds = CredentialFactory.getCredentialFromClientSecrets(secretsFile, appName);
    Storage.Builder builder = new Storage.Builder(httpTransport, JSON_FACTORY, null)
        .setApplicationName(appName);
    storage = factory.fromCredential(builder, creds).build();
  }

  @VisibleForTesting
  GCSHelper() {
  }

  /**
   * Get the underlying GCS Storage object, for advanced uses
   * (e.g. a download progressbar).
   */
  public Storage getStorage() {
    return this.storage;
  }

  /**
   * @param name of the file we're interested in
   * @return size of the file, in bytes
   * @throws IOException
   */
  public long getFileSize(String bucket, String name) throws IOException {
    Storage.Objects.Get getObject = storage.objects().get(bucket, name);
    StorageObject object = getObject.execute();
    BigInteger size = object.getSize();
    if (size.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
      throw new RuntimeException("File size is too big for a long!");
    }
    return size.longValue();
  }


  /**
   * Retrieve part of the file.
   *
   * @throws IOException
   */
  public ByteArrayOutputStream getPartialObjectData(String bucket, String fname, long start, long endIncl) throws IOException {
    return getPartialObjectData(bucket, fname, start, endIncl, null);
  }

  /**
   * Retrieve part of the file.
   *
   * Example thing you may want to do with the result:
   * String str = new String( Arrays.copyOfRange(out.toByteArray() );
   *
   * @throws IOException
   */
  public ByteArrayOutputStream getPartialObjectData(String bucket, String fname, long start, long endIncl,
                                                    @Nullable ByteArrayOutputStream optionalOldOutputToReuse) throws IOException {
    ByteArrayOutputStream out;
    if (null == optionalOldOutputToReuse) {
      out = new ByteArrayOutputStream((int) (endIncl - start + 1));
    } else {
      out = optionalOldOutputToReuse;
      out.reset();
    }
    Storage.Objects.Get getObject = storage.objects().get(bucket, fname);

    getObject.setRequestHeaders(new HttpHeaders().setRange(
        String.format("bytes=%d-%d", start, endIncl)));

    getObject.getMediaHttpDownloader().setDirectDownloadEnabled(!IS_APP_ENGINE);
    getObject.executeMediaAndDownloadTo(out);

    if (out.size() != (endIncl - start + 1)) {
      String err = "getPartialObjectData failed! Expected " + (endIncl - start + 1) + " bytes, got " + out.size();
      LOGGER.log(Level.WARNING, err);
      throw new IOException(err);
    }

    return out;
  }

  /**
   * Retrieve the whole file (to memory).
   *
   * @throws IOException
   */
  public InputStream getWholeObject(String bucket, String fname) throws IOException {
    Storage.Objects.Get getObject = storage.objects().get(bucket, fname);
    return getObject.executeMediaAsInputStream();
  }

  /**
   * Retrieve the whole file (to a temporary file on disk).
   *
   * @throws IOException
   */
  public File getAsFile(String bucket, String fname) throws IOException {
    Storage.Objects.Get request = storage.objects().get(bucket, fname);
    File file = File.createTempFile("gcsdownload", "obj");
    try (OutputStream out = new FileOutputStream(file)) {
      request.executeMediaAndDownloadTo(out);
    }
    return file;
  }

}
