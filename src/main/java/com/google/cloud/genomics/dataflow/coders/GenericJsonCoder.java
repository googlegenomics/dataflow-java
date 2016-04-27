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

import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderProvider;
import com.google.cloud.dataflow.sdk.coders.Proto2Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.protobuf.Message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.Serializable;

/**
 * Can be used as a coder for any object that extends GenericJson.
 * This includes all objects in the Google Genomics Java client library.
*/
public class GenericJsonCoder<T extends GenericJson> extends DelegatingAtomicCoder<T, String> {

  private static final JsonFactory JSON_FACTORY = Utils.getDefaultJsonFactory();
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  public static <T extends GenericJson> GenericJsonCoder<T> of(Class<T> type) {
    return new GenericJsonCoder<>(type);
  }

  @JsonCreator
  @SuppressWarnings("unchecked")
  public static <T extends GenericJson> GenericJsonCoder<T> of(@JsonProperty("type") String type)
      throws ClassNotFoundException {
    return of((Class<T>) Class.forName(type));
  }

  private final Class<T> type;

  private GenericJsonCoder(Class<T> type) {
    super(STRING_CODER);
    this.type = type;
  }

  @Override
  public CloudObject asCloudObject() {
    CloudObject result = super.asCloudObject();
    result.put("type", type.getName());
    return result;
  }

  @Override protected T from(String object) throws IOException {
    return JSON_FACTORY.fromString(object, type);
  }

  @Override protected String to(T object) throws IOException {
    return JSON_FACTORY.toString(object);
  }

  /**
   * Coder provider for all objects in the Google Genomics Java client library.
   */
  public static final CoderProvider PROVIDER = new CoderProvider() {
    @Override
    @SuppressWarnings("unchecked")
    public <T> Coder<T> getCoder(TypeDescriptor<T> typeDescriptor)
        throws CannotProvideCoderException {
      Class<T> rawType = (Class<T>) typeDescriptor.getRawType();
      if (!GenericJson.class.isAssignableFrom(rawType)) {
        if (Message.class.isAssignableFrom(rawType)) {
          return (Coder<T>) Proto2Coder.of((Class<? extends Message>) rawType);
        } else if (Serializable.class.isAssignableFrom(rawType)) {
          // Fall back this here because if this is used as the follback coder, it overwrites the
          // default fallback CoderProvider of SerializableCoder.PROVIDER.
          return (Coder<T>) SerializableCoder.of((Class<? extends Serializable>) rawType);
        } else {
          throw new CannotProvideCoderException("Class " + rawType
              + " does not implement GenericJson, Message, or Serializable");
        }
      }
      return (Coder<T>) GenericJsonCoder.of((Class<? extends GenericJson>) rawType);
    }
  };
}
