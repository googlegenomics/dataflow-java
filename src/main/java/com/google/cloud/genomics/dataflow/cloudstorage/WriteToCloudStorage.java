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

import com.google.api.client.http.FileContent;
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
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.common.base.Optional;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;

public abstract class WriteToCloudStorage<T>
    extends PTransform<WriteToCloudStorage.Input<T>, PObject<StorageObject>> {

  public static class Input<T> extends AbstractPInput<Input<T>> {

    public static <T> Input<T> of(
        PObject<ApiFactory> apiFactory,
        PObject<StorageObject> object,
        PCollection<? extends T> collection) {
      return new Input<>(apiFactory, object, collection);
    }

    private final PObject<ApiFactory> apiFactory;
    private final PCollection<? extends T> collection;
    private final PObject<StorageObject> object;

    private Input(
        PObject<ApiFactory> apiFactory,
        PObject<StorageObject> object,
        PCollection<? extends T> collection) {
      this.apiFactory = apiFactory;
      this.object = object;
      this.collection = collection;
    }

    public PObject<ApiFactory> apiFactory() {
      return apiFactory;
    }

    public PCollection<? extends T> collection() {
      return collection;
    }

    @Override public Collection<? extends PValue> expand() {
      return Arrays.asList(apiFactory(), object(), collection());
    }

    public PObject<StorageObject> object() {
      return object;
    }
  }

  private final TupleTag<ApiFactory> apiFactoryTag = new TupleTag<>();
  private final TupleTag<StorageObject> objectTag = new TupleTag<>();

  @Override public PObject<StorageObject> apply(Input<T> input) {
    return input
        .collection()
        .apply(ParDo
            .withSideInputs(PObjectTuple
                .of(apiFactoryTag, input.apiFactory())
                .and(objectTag, input.object()))
            .of(
                new DoFn<T, StorageObject>() {

                  private String contentType;
                  private File file;
                  private StorageObject object;
                  private OutputStream out;

                  @Override public void finishBatch(Context context) throws IOException {
                    out.close();
                    context.output(context
                        .sideInput(apiFactoryTag)
                        .createApi(StorageApiFactoryImplementation.INSTANCE)
                        .objects()
                        .insert(object.getBucket(), object, new FileContent(contentType, file))
                        .execute());
                  }

                  @Override public void processElement(ProcessContext context) throws IOException {
                    serialize(context.element(), out);
                  }

                  @Override public void startBatch(Context context) throws IOException {
                    out = Compression
                        .forContentType(contentType = Optional
                            .fromNullable((object = context.sideInput(objectTag)).getContentType())
                            .or("application/octet-stream"))
                        .wrap(new FileOutputStream(file = File.createTempFile(getName(), ".tmp")));
                    file.deleteOnExit();
                  }
                }))
        .setCoder(GenericJsonCoder.of(StorageObject.class))
        .apply(Convert.<StorageObject>fromSingleton());
  }

  protected abstract void serialize(T object, OutputStream out) throws IOException;
}