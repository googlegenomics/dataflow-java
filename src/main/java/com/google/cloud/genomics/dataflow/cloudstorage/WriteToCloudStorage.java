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
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.transforms.Convert;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.dataflow.sdk.values.PObjectTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.ProxyPValue;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class WriteToCloudStorage<T>
    extends PTransform<WriteToCloudStorage.Input<T>, PObject<StorageObject>> {

  public static class Input<T> extends ProxyPValue<PCollection<T>, Input<T>> {

    public static <T> Input<T> of(
        PCollection<T> records,
        PObject<String> bucket,
        PObject<StorageObject> object) {
      return new Input<>(records, bucket, object);
    }

    private final PObject<String> bucket;
    private final PObject<StorageObject> object;

    private Input(PCollection<T> records, PObject<String> bucket, PObject<StorageObject> object) {
      super(records);
      this.bucket = bucket;
      this.object = object;
    }

    PCollection<T> records() {
      return delegate();
    }

    PObject<String> bucket() {
      return bucket;
    }

    PObject<StorageObject> object() {
      return object;
    }
  }

  private final String applicationName;
  private final TupleTag<String> bucketTag = new TupleTag<>();
  private final TupleTag<StorageObject> objectTag = new TupleTag<>();

  protected WriteToCloudStorage(String applicationName) {
    this.applicationName = applicationName;
  }

  @Override public PObject<StorageObject> apply(Input<T> input) {
    return input
        .records()
        .apply(ParDo
            .withSideInputs(PObjectTuple
                .of(bucketTag, input.bucket())
                .and(objectTag, input.object()))
            .of(
                new StorageDoFn<T, StorageObject>(applicationName) {

                  private String bucket;
                  private StorageObject object;
                  private OutputStream out;
                  private File uncompressed;

                  @Override protected void finishBatch(Storage storage, Context context)
                      throws IOException {
                    out.close();
                    String contentType = Optional
                        .fromNullable(object.getContentType())
                        .or("binary/octet-stream");
                    context.output(storage
                        .objects()
                        .insert(
                            bucket,
                            object,
                            new FileContent(
                                contentType,
                                compress(Compression.forContentType(contentType), uncompressed)))
                        .execute());
                  }

                  @Override protected void processElement(Storage storage, ProcessContext context)
                      throws IOException {
                    serialize(context.element(), out);
                  }

                  @Override protected void startBatch(Storage storage, Context context)
                      throws IOException {
                    bucket = context.sideInput(bucketTag);
                    object = context.sideInput(objectTag);
                    out = new FileOutputStream(uncompressed = createTempFile("uncompressed"));
                  }
                }))
        .setCoder(GenericJsonCoder.of(StorageObject.class))
        .apply(Convert.<StorageObject>fromSingleton());
  }

  private static File compress(Compression compression, File uncompressed) throws IOException {
    if (Compression.UNCOMPRESSED == compression) {
      return uncompressed;
    }
    File compressed = createTempFile("compressed");
    try (InputStream in = new FileInputStream(uncompressed)) {
      try (OutputStream out = compression.wrap(new FileOutputStream(compressed))) {
        ByteStreams.copy(in, out);
      }
    }
    return compressed;
  }

  private static File createTempFile(String prefix) throws IOException {
    File tempFile = File.createTempFile(prefix, ".tmp");
    tempFile.deleteOnExit();
    return tempFile;
  }

  protected abstract void serialize(T object, OutputStream out) throws IOException;
}