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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.transforms.Convert;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.dataflow.sdk.values.PObjectTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Optional;

public class StorageTransforms {

  public static class Objects {

    public static class Get
        extends PTransform<PCollection<Get.Request>, PCollection<StorageObject>> {

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
          if (null != obj && Request.class == obj) {
            Request rhs = (Request) obj;
            return java.util.Objects.equals(bucket(), rhs.bucket())
                && java.util.Objects.equals(name(), rhs.name());
          }
          return false;
        }

        @Override public int hashCode() {
          return java.util.Objects.hash(bucket(), name());
        }

        public String name() {
          return name;
        }
      }

      public static Get of(String applicationName) {
        return new Get(applicationName);
      }

      private final String applicationName;

      private Get(String applicationName) {
        this.applicationName = applicationName;
      }

      @Override public PCollection<StorageObject> apply(PCollection<Request> input) {
        return input.apply(ParDo.named("StorageTransforms.Objects.Get").of(
            new ObjectsDoFn<Request, StorageObject>(applicationName) {
              @Override protected void processElement(
                  Storage.Objects objects, ProcessContext context) throws IOException {
                Request request = context.element();
                context.output(objects.get(request.bucket(), request.name()).execute());
              }
            }));
      }
    }

    public abstract static class GetMedia<T>
        extends PTransform<PCollection<StorageObject>, PCollection<T>> {

      private final String applicationName;

      protected GetMedia(String applicationName) {
        this.applicationName = applicationName;
      }

      @Override public final PCollection<T> apply(PCollection<StorageObject> input) {
        return input
            .apply(ParDo.named("StorageTransforms.Objects.GetMedia").of(
                new ObjectsDoFn<StorageObject, T>(applicationName) {
                  @Override protected void processElement(
                      Storage.Objects objects, ProcessContext context) throws IOException {
                    StorageObject object = context.element();
                    for (T t : deserialize(objects.get(object.getBucket(),
                        object.getName()).executeMediaAsInputStream())) {
                      context.output(t);
                    }
                  }
                }))
            .setOrdered(true);
      }

      protected abstract Iterable<T> deserialize(InputStream in) throws IOException;

    }

    public abstract static class Insert<T>
        extends PTransform<Insert.Input<T>, PObject<StorageObject>> {

      public static class Input<T> extends AbstractPValue<T, Input<T>> {

        public static <T> Input<T> of(
            PCollection<T> collection,
            PObject<String> bucket,
            PObject<StorageObject> object,
            PObject<String> contentType) {
          return new Input<>(
              collection,
              Optional.of(PObjectTuple
                  .of(BUCKET_TAG, bucket)
                  .and(OBJECT_TAG, object)
                  .and(CONTENT_TYPE_TAG, contentType)));
        }

        private Input(PCollection<T> collection, Optional<PObjectTuple> sideInputs) {
          super(collection, sideInputs);
        }
      }

      static final TupleTag<String> BUCKET_TAG = new TupleTag<>();
      static final TupleTag<String> CONTENT_TYPE_TAG = new TupleTag<>();
      static final TupleTag<StorageObject> OBJECT_TAG = new TupleTag<>();

      private final String applicationName;

      protected Insert(String applicationName) {
        this.applicationName = applicationName;
      }

      @Override public PObject<StorageObject> apply(Input<T> input) {
        return input.collection().apply(ParDo
            .named("StorageTransforms.Objects.Insert")
            .withSideInputs(input.sideInputs().get())
            .of(
                new ObjectsDoFn<T, StorageObject>(applicationName) {

                  private ByteArrayOutputStream buffer;

                  @Override protected void finishBatch(Storage.Objects objects, Context context)
                      throws IOException {
                    context.output(objects
                        .insert(
                            context.sideInput(BUCKET_TAG),
                            context.sideInput(OBJECT_TAG),
                            new ByteArrayContent(
                                context.sideInput(CONTENT_TYPE_TAG),
                                buffer.toByteArray()))
                        .execute());
                  }

                  @Override protected void processElement(
                      Storage.Objects objects, ProcessContext context) throws IOException {
                    serialize(context.element(), buffer);
                  }

                  @Override protected void startBatch(Storage.Objects objects, Context context) {
                    buffer = new ByteArrayOutputStream();
                  }
                }))
            .apply(Convert.<StorageObject>fromSingleton());
      }

      protected abstract void serialize(T object, OutputStream out) throws IOException;
    }

    abstract static class ObjectsDoFn<I, O> extends StorageDoFn<I, O> {

      private Storage.Objects objects;

      ObjectsDoFn(String applicationName) {
        super(applicationName);
      }

      @Override protected final void finishBatch(Storage storage, Context context)
          throws Exception {
        finishBatch(objects, context);
      }

      protected void finishBatch(Storage.Objects objects, Context context) throws Exception {}

      @Override protected final void processElement(Storage storage, ProcessContext context)
          throws Exception {
        processElement(objects, context);
      }

      protected abstract void processElement(Storage.Objects objects, ProcessContext context)
          throws Exception;

      @Override protected final void startBatch(Storage storage, Context context) throws Exception {
        startBatch(objects = storage.objects(), context);
      }

      protected void startBatch(Storage.Objects objects, Context context) throws Exception {}
    }
  }

  abstract static class StorageDoFn<I, O> extends ApiDoFn<Storage, Storage.Builder, I, O> {

    StorageDoFn(String applicationName) {
      super(applicationName);
    }

    @Override protected final Storage build(Storage.Builder builder) {
      return builder.build();
    }

    @Override protected final Storage.Builder newBuilder(
        HttpTransport httpTransport,
        JsonFactory jsonFactory,
        HttpRequestInitializer requestInitializer) {
      return new Storage.Builder(httpTransport, jsonFactory, requestInitializer);
    }
  }
}