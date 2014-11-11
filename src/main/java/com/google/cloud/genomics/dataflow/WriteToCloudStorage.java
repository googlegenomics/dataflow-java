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

import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.transforms.Convert;
import com.google.cloud.dataflow.sdk.transforms.CreatePObject;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.dataflow.sdk.values.PObjectTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public abstract class WriteToCloudStorage<T> implements Serializable {

  protected abstract void serialize(T object, OutputStream out) throws IOException;

  public final PObject<StorageObject> write(
      PObject<JsonClientFactory> jsonClientFactory,
      PObject<StorageObject> object,
      PCollection<T> collection) {
    final TupleTag<JsonClientFactory> jsonClientFactoryTag = new TupleTag<>();
    final TupleTag<StorageObject> objectTag = new TupleTag<>();
    return collection
        .apply(ParDo
            .withSideInputs(PObjectTuple
                .of(jsonClientFactoryTag, jsonClientFactory)
                .and(objectTag, object))
            .of(
                new DoFn<T, StorageObject>() {

                  private Future<StorageObject> future;
                  private PipedOutputStream out;

                  @Override public void finishBatch(Context context)
                      throws ExecutionException, InterruptedException, IOException {
                    out.close();
                    context.output(future.get());
                  }

                  @Override public void processElement(ProcessContext context) throws IOException {
                    serialize(context.element(), out);
                  }

                  @Override public void startBatch(final Context context) {
                    final ExecutorService executorService = Executors.newSingleThreadExecutor();
                    future = executorService.submit(
                        new Callable<StorageObject>() {
                          @Override public StorageObject call() throws IOException {
                            try (PipedInputStream
                                in = new PipedInputStream(out = new PipedOutputStream())) {
                              StorageObject object = context.sideInput(objectTag);
                              return context
                                  .sideInput(jsonClientFactoryTag)
                                  .createClient(StorageImplementation.INSTANCE)
                                  .objects()
                                  .insert(
                                      object.getBucket(),
                                      object,
                                      new InputStreamContent(object.getContentType(), in))
                                  .execute();
                            } finally {
                              executorService.shutdown();
                            }
                          }
                        });
                  }
                }))
        .setCoder(GenericJsonCoder.of(StorageObject.class))
        .apply(Convert.<StorageObject>fromSingleton());
  }

  public final PObject<StorageObject> write(
      PObject<JsonClientFactory> jsonClientFactory,
      StorageObject object,
      PCollection<T> collection) {
    return write(
        jsonClientFactory,
        jsonClientFactory
            .getPipeline()
            .apply(CreatePObject.of(object))
            .setCoder(GenericJsonCoder.of(StorageObject.class)),
        collection);
  }
}