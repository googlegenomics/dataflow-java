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

import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.transforms.Convert;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.dataflow.sdk.values.PObjectTuple;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.AbstractPInput;
import com.google.cloud.genomics.dataflow.ApiFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;

public abstract class ReadFromCloudStorage<T>
    extends PTransform<ReadFromCloudStorage.Input, PCollection<T>> {

  public static class Input extends AbstractPInput<Input> {

    public static Input of(PObject<ApiFactory> apiFactory, PObject<StorageObject> object) {
      return new Input(apiFactory, object);
    }

    private final PObject<ApiFactory> apiFactory;
    private final PObject<StorageObject> object;

    private Input(PObject<ApiFactory> apiFactory, PObject<StorageObject> object) {
      this.apiFactory = apiFactory;
      this.object = object;
    }

    public PObject<ApiFactory> apiFactory() {
      return apiFactory;
    }

    @Override public Collection<? extends PValue> expand() {
      return Arrays.asList(apiFactory(), object());
    }

    public PObject<StorageObject> object() {
      return object;
    }
  }

  private final TupleTag<ApiFactory> apiFactoryTag = new TupleTag<>();

  @Override public PCollection<T> apply(Input input) {
    return input
        .object()
        .apply(Convert.<StorageObject>toSingleton())
        .apply(ParDo
            .withSideInputs(PObjectTuple.of(apiFactoryTag, input.apiFactory()))
            .of(
                new DoFn<StorageObject, T>() {
                  @Override public void processElement(ProcessContext context) throws IOException {
                    StorageObject object = context.element();
                    for (T output : deserialize(Compression
                        .forContentType(object.getContentType())
                        .wrap(context
                            .sideInput(apiFactoryTag)
                            .createApi(StorageApiFactoryImplementation.INSTANCE)
                            .objects()
                            .get(object.getBucket(), object.getName())
                            .executeMediaAsInputStream()))) {
                      context.output(output);
                    }
                  }
                }))
        .setOrdered(true);
  }

  protected abstract Iterable<T> deserialize(InputStream in) throws IOException;
}