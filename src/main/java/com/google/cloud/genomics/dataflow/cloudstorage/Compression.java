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
package com.google.cloud.genomics.dataflow.cloudstorage;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public enum Compression {

  GZIP {

    @Override public Optional<String> contentType() {
      return Optional.of("application/gzip");
    }

    @Override public InputStream wrap(InputStream in) throws IOException {
      return new GZIPInputStream(in);
    }

    @Override public OutputStream wrap(OutputStream out) throws IOException {
      return new GZIPOutputStream(out);
    }
  },

  UNCOMPRESSED {

    @Override public Optional<String> contentType() {
      return Optional.absent();
    }

    @Override public InputStream wrap(InputStream in) {
      return in;
    }

    @Override public OutputStream wrap(final OutputStream out) {
      return out;
    }
  };

  private static final Map<Optional<String>, Compression> COMPRESSION_BY_CONTENT_TYPE =
      Maps.uniqueIndex(
          Arrays.asList(values()),
          new Function<Compression, Optional<String>>() {
            @Override public Optional<String> apply(Compression compression) {
              return compression.contentType();
            }
          });

  public static Compression forContentType(String contentType) {
    return Optional
        .fromNullable(COMPRESSION_BY_CONTENT_TYPE.get(Optional.fromNullable(contentType)))
        .or(UNCOMPRESSED);
  }

  public abstract Optional<String> contentType();

  public abstract InputStream wrap(InputStream in) throws IOException;

  public abstract OutputStream wrap(OutputStream out) throws IOException;
}