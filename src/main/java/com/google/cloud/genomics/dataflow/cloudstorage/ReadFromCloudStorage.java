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
package com.google.cloud.genomics.dataflow.cloudstorage;

import com.google.api.services.storage.Storage;
import com.google.cloud.dataflow.sdk.transforms.Convert;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Objects;

public abstract class ReadFromCloudStorage<T>
    extends PTransform<PObject<ReadFromCloudStorage.Request>, PCollection<T>> {

  public static class Request implements Serializable {

    public static Request of(String bucket, String name) {
      return new Request(bucket, name);
    }

    private final String bucket;
    private final String name;

    private Request(String bucket, String name) {
      this.bucket = bucket;
      this.name = name;
    }

    public String bucket() {
      return bucket;
    }

    @Override public boolean equals(Object obj) {
      if (null != obj && Request.class == obj.getClass()) {
        Request rhs = (Request) obj;
        return Objects.equals(bucket(), rhs.bucket()) && Objects.equals(name(), rhs.name());
      }
      return false;
    }

    @Override public int hashCode() {
      return Objects.hash(bucket(), name());
    }

    public String name() {
      return name;
    }
  }

  private final String applicationName;

  protected ReadFromCloudStorage(String applicationName) {
    this.applicationName = applicationName;
  }

  @Override public PCollection<T> apply(PObject<Request> inputs) {
    return inputs
        .apply(Convert.<Request>toSingleton())
        .apply(ParDo.of(
            new StorageDoFn<Request, T>(applicationName) {
              @Override protected void processElement(Storage storage, ProcessContext context)
                  throws IOException {
                Storage.Objects objects = storage.objects();
                Request request = context.element();
                String bucket = request.bucket();
                String name = request.name();
                try (InputStream in = Compression
                    .forContentType(objects.get(bucket, name).execute().getContentType())
                    .wrap(objects.get(bucket, name).executeMediaAsInputStream())) {
                  for (T object : deserialize(in)) {
                    context.output(object);
                  }
                }
              }
            }))
        .setOrdered(true);
  }

  protected abstract Iterable<T> deserialize(InputStream in) throws IOException;
}