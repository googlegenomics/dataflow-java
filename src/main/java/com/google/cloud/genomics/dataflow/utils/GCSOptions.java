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

import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.dataflow.DataflowScopes;
import com.google.api.services.genomics.GenomicsScopes;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.common.collect.ImmutableList;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.logging.Logger;

/**
 * Options for pipelines that need to access GCS storage.
 * Users must call Methods.initialize to get the credentials set up.
 * getStorageClient() provides the GCS storage client.
 */
public interface GCSOptions extends GenomicsOptions {
  class HttpTransportFactory implements DefaultValueFactory<HttpTransport> {
    @Override
    public HttpTransport create(PipelineOptions options) {
      return Utils.getDefaultTransport();
    }
  }
  
  @Default.InstanceFactory(HttpTransportFactory.class)
  @JsonIgnore
  HttpTransport getTransport();

  void setTransport(HttpTransport transport);

  class JsonFactoryFactory implements DefaultValueFactory<JsonFactory> {
    @Override
    public JsonFactory create(PipelineOptions options) {
      return Utils.getDefaultJsonFactory();
    }
  }
  
  @Default.InstanceFactory(JsonFactoryFactory.class)
  @JsonIgnore
  JsonFactory getJsonFactory();

  void setJsonFactory(JsonFactory jsonFactory);

  
  class ScopesFactory implements DefaultValueFactory<List<String>> {
    @Override
    public List<String> create(PipelineOptions options) {
      return ImmutableList.<String>builder()
        .add(DataflowScopes.USERINFO_EMAIL)
        .add(GenomicsScopes.GENOMICS)
        .add(StorageScopes.DEVSTORAGE_READ_WRITE)
        .build();
    }
  }
  
  @Default.InstanceFactory(ScopesFactory.class)
  @JsonIgnore
  List<String> getScopes();

  void setScopes(List<String> scopes);
  
  class GenomicsFactoryFactory implements DefaultValueFactory<GenomicsFactory> {
    @Override
    public GenomicsFactory create(PipelineOptions options) {
      GCSOptions gcsOptions = options.as(GCSOptions.class);
      try {
        return GenomicsFactory
          .builder(gcsOptions.getAppName())
            .setScopes(gcsOptions.getScopes())
            .setHttpTransport(gcsOptions.getTransport())
            .setJsonFactory(gcsOptions.getJsonFactory())
            .build();
      } catch (Exception ex) {
          throw new RuntimeException(ex);
      }
    }
  }
  
  @Default.InstanceFactory(GenomicsFactoryFactory.class)
  @JsonIgnore
  GenomicsFactory getGenomicsFactory();
  
  void setGenomicsFactory(GenomicsFactory factory);
  
  class Methods {
    private static final Logger LOG = Logger.getLogger(GCSOptions.class.getName());
    private Methods() {
    }
    
    public static GenomicsFactory.OfflineAuth createGCSAuth(GCSOptions options)
      throws IOException, GeneralSecurityException {
      return GenomicsOptions.Methods.getGenomicsAuth(options);
    }
    
    public static Storage.Objects createStorageClient(
        DoFn<?, ?>.Context context, GenomicsFactory.OfflineAuth auth) throws IOException {
      final GCSOptions gcsOptions =
          context.getPipelineOptions().as(GCSOptions.class);
      return createStorageClient(gcsOptions, auth);
    }
    
    public static Storage.Objects createStorageClient(GCSOptions gcsOptions,
        GenomicsFactory.OfflineAuth auth) throws IOException {
      LOG.info("Creating storgae client for " + auth.applicationName);
      final Storage.Builder storageBuilder = new Storage.Builder(
          gcsOptions.getTransport(),
          gcsOptions.getJsonFactory(),
          null);
      
      return auth
          .setupAuthentication(gcsOptions.getGenomicsFactory(), storageBuilder)
          .build()
            .objects();
      }
    
  }
}
