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

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.Convert;
import com.google.cloud.dataflow.sdk.transforms.CreatePObject;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.dataflow.sdk.values.PObjectTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.common.base.Optional;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

public abstract class ReadFromCloudStorage<T> implements Serializable {

  private static PObject<StorageObject> createPObject(Pipeline pipeline, StorageObject object) {
    return pipeline
        .apply(CreatePObject.of(object))
        .setCoder(GenericJsonCoder.of(StorageObject.class));
  }

  protected abstract Iterable<T> deserialize(InputStream in) throws IOException;

  public final PCollection<T> read(
      PObject<JsonClientFactory> jsonClientFactory,
      PObject<StorageObject> object) {
    return read(jsonClientFactory, object, Optional.<Coder<T>>absent());
  }

  public final PCollection<T> read(
      PObject<JsonClientFactory> jsonClientFactory,
      PObject<StorageObject> object,
      Coder<T> coder) {
    return read(jsonClientFactory, object, Optional.of(coder));
  }

  private PCollection<T> read(
      PObject<JsonClientFactory> jsonClientFactory,
      PObject<StorageObject> object,
      Optional<Coder<T>> coder) {
    final TupleTag<JsonClientFactory> jsonClientFactoryTag = new TupleTag<>();
    PCollection<T> result = object
        .apply(Convert.<StorageObject>toSingleton())
        .apply(ParDo
            .withSideInputs(PObjectTuple.of(jsonClientFactoryTag, jsonClientFactory))
            .of(
                new DoFn<StorageObject, T>() {
                  @Override public void processElement(ProcessContext context) throws IOException {
                    StorageObject storageObject = context.element();
                    Optional<String> contentEncoding =
                        Optional.fromNullable(storageObject.getContentEncoding());
                    Storage.Objects.Get request = context
                        .sideInput(jsonClientFactoryTag)
                        .createClient(StorageImplementation.INSTANCE)
                        .objects()
                        .get(storageObject.getBucket(), storageObject.getName());
                    try (InputStream in = (contentEncoding.isPresent()
                        ? request.set("Accept-Encoding", contentEncoding.get())
                        : request).executeMediaAsInputStream()) {
                      for (T object : deserialize(in)) {
                        context.output(object);
                      }
                    }
                  }
                }))
        .setOrdered(true);
    if (coder.isPresent()) {
      result.setCoder(coder.get());
    }
    return result;
  }

  public final PCollection<T> read(
      PObject<JsonClientFactory> jsonClientFactory,
      StorageObject object) {
    return read(
        jsonClientFactory,
        createPObject(jsonClientFactory.getPipeline(), object),
        Optional.<Coder<T>>absent());
  }

  public final PCollection<T> read(
      PObject<JsonClientFactory> jsonClientFactory,
      StorageObject object,
      Coder<T> coder) {
    return read(
        jsonClientFactory,
        createPObject(jsonClientFactory.getPipeline(), object),
        Optional.of(coder));
  }
}