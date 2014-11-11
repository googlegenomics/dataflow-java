/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dataflow;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.storage.Storage;
import com.google.cloud.genomics.utils.JsonClientBuilder;

/** For use with {@link JsonClientBuilder#build} */
public enum StorageImplementation
    implements JsonClientBuilder.Implementation<Storage, Storage.Builder> {

  INSTANCE;

  @Override public Storage build(Storage.Builder builder) {
    return builder.build();
  }

  @Override public Storage.Builder newBuilder(
      HttpTransport transport,
      JsonFactory jsonFactory,
      HttpRequestInitializer requestInitializer) {
    return new Storage.Builder(transport, jsonFactory, requestInitializer);
  }
}