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
package com.google.cloud.genomics.dataflow;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.genomics.Genomics;
import com.google.cloud.genomics.utils.JsonClientFactory;

import java.util.Collections;

public abstract class GenomicsDoFn<I, O> extends ApiDoFn<Genomics, Genomics.Builder, I, O> {

  protected GenomicsDoFn(final String applicationName) {
    super(
        new JsonClientFactory.Logic<Genomics, Genomics.Builder>() {

          @Override public Genomics build(Genomics.Builder builder) {
            return builder
                .setApplicationName(applicationName)
                .build();
          }

          @Override public Genomics.Builder newBuilder(HttpTransport httpTransport,
              JsonFactory jsonFactory, HttpRequestInitializer requestInitializer) {
            return new Genomics.Builder(httpTransport, jsonFactory, requestInitializer);
          }
        });
  }

  @Override protected final Iterable<String> additionalScopes() {
    return Collections.singletonList("https://www.googleapis.com/auth/genomics");
  }
}