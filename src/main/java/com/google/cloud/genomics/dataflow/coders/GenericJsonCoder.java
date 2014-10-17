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
package com.google.cloud.genomics.dataflow.coders;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.model.CloudNamedParameter;
import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * Can be used as a coder for any object that extends GenericJson.
 * This includes all objects in the Google Genomics Java client library.
*/
public class GenericJsonCoder<T extends GenericJson> extends AtomicCoder<T> {

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  public static <T extends GenericJson> GenericJsonCoder<T> of(Class<T> type) {
    return new GenericJsonCoder<>(type);
  }

  @JsonCreator
  @SuppressWarnings("unchecked")
  public static <T extends GenericJson> GenericJsonCoder<T> of(@JsonProperty("type") String type)
      throws ClassNotFoundException {
    return of((Class<T>) Class.forName(type));
  }

  private Coder<String> delegate = StringUtf8Coder.of();
  private final Class<T> type;

  private GenericJsonCoder(Class<T> type) {
    this.type = type;
  }

  @Override protected void addCloudEncodingDetails(Map<String, CloudNamedParameter> details) {
    details.put("type", new CloudNamedParameter().setStringValue(type.getName()));
  }

  @Override public T decode(InputStream inStream, Context context)
      throws CoderException, IOException {
    return JSON_FACTORY.fromString(delegate.decode(inStream, context), type);
  }

  @Override public void encode(T value, OutputStream outStream, Context context)
      throws CoderException, IOException {
    delegate.encode(JSON_FACTORY.toString(value), outStream, context);
  }

  @Override public boolean isDeterministic() {
    return delegate.isDeterministic();
  }
}
