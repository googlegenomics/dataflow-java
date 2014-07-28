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
import com.google.api.client.json.JsonGenerator;
import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.model.CloudNamedParameter;
import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.CoderException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Can be used as a coder for any object that extends GenericJson.
 * This includes all objects in the Google Genomics Java client library.
*/
public class GenericJsonCoder<T extends GenericJson> extends AtomicCoder<T> {
  private static final int READ_LIMIT = 1 << 22; // 4MB buffer for read limit
  private static JacksonFactory jacksonFactory = new JacksonFactory();

  public static <T extends GenericJson> GenericJsonCoder<T> of(Class<T> type) {
    return new GenericJsonCoder<>(type);
  }

  @SuppressWarnings("unchecked")
  @JsonCreator
  public static GenericJsonCoder<? > of(@JsonProperty("type") String classType)
      throws ClassNotFoundException {
    return of((Class<? extends GenericJson>) Class.forName(classType));
  }

  private final Class<T> type;

  protected GenericJsonCoder(Class<T> type) {
    this.type = type;
  }

  @Override
  public void encode(T value, OutputStream out, Context context) throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null record");
    }

    JsonGenerator generator = jacksonFactory.createJsonGenerator(out, Charset.defaultCharset());
    generator.serialize(value);
    generator.flush();
  }

  @Override
  public T decode(InputStream in, Context context) throws IOException {
    if (context.isWholeStream) {
      JsonParser jsonParser = jacksonFactory.createJsonParser(in);
      T obj = jsonParser.parse(type);
      jsonParser.close();
      return obj;
    } else {
      /* 
       * JsonParser reads past the end of the object in the InputStream by a large number of bytes.
       * When coded in an Iterable, the stream contains concatenated json objects, so this will 
       * cause errors. To fix this, we allow the inputstream to backtrack READ_LIMIT bytes and 
       * move it to the end of the read object before closing.
       * 
       * NOTE:
       * If encoded Json object is more than READ_LIMIT bytes and is in iterable, this will fail.
       */
      
      in.mark(READ_LIMIT);
      JsonParser jsonParser = jacksonFactory.createJsonParser(in);
      T obj = jsonParser.parse(type);

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      encode(obj, buf, context);
      int skip = buf.size();
      in.reset();
      in.skip(skip);
      
      jsonParser.close();
      return obj;
    }
  }

  @Override
  protected void addCloudEncodingDetails(
      Map<String, CloudNamedParameter> encodingParameters) {
    encodingParameters.put("type",
        new CloudNamedParameter().setStringValue(type.getName()));
  }

  @Override
  public boolean isDeterministic() {
    return true;
  }
}
