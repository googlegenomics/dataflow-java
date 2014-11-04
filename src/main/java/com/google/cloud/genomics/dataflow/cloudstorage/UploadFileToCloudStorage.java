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

import com.google.api.client.http.FileContent;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.transforms.Convert;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.dataflow.sdk.values.PObjectTuple;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.AbstractPInput;
import com.google.cloud.genomics.dataflow.ApiFactory;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;

public final class UploadFileToCloudStorage
    extends PTransform<UploadFileToCloudStorage.Input, PObject<StorageObject>> {

  public static final class Input extends AbstractPInput<Input> {

    public static Input of(
        PObject<ApiFactory> apiFactory,
        PObject<File> file,
        PObject<StorageObject> object) {
      return new Input(apiFactory, file, object);
    }

    private final PObject<ApiFactory> apiFactory;

    private final PObject<File> file;
    private final PObject<StorageObject> object;
    private Input(
        PObject<ApiFactory> apiFactory,
        PObject<File> file,
        PObject<StorageObject> object) {
      this.apiFactory = apiFactory;
      this.file = file;
      this.object = object;
    }

    public PObject<ApiFactory> apiFactory() {
      return apiFactory;
    }

    @Override public Collection<? extends PValue> expand() {
      return Arrays.asList(apiFactory(), file(), object());
    }

    public PObject<File> file() {
      return file;
    }

    public PObject<StorageObject> object() {
      return object;
    }
  }

  private static final UploadFileToCloudStorage INSTANCE = new UploadFileToCloudStorage();

  public static UploadFileToCloudStorage of() {
    return INSTANCE;
  }

  private final TupleTag<ApiFactory> apiFactoryTag = new TupleTag<>();
  private final TupleTag<StorageObject> objectTag = new TupleTag<>();

  private UploadFileToCloudStorage() {}

  @Override public PObject<StorageObject> apply(Input input) {
    return input
        .file()
        .apply(Convert.<File>toSingleton())
        .apply(ParDo
            .withSideInputs(PObjectTuple
                .of(apiFactoryTag, input.apiFactory())
                .and(objectTag, input.object()))
            .of(
                new DoFn<File, StorageObject>() {

                  private File compress(Compression compression, File uncompressed)
                      throws IOException {
                    if (Compression.UNCOMPRESSED != compression) {
                      File compressed = File.createTempFile(uncompressed.getName(), ".tmp");
                      compressed.deleteOnExit();
                      try (InputStream in = new FileInputStream(uncompressed)) {
                        try (OutputStream out =
                            compression.wrap(new FileOutputStream(compressed))) {
                          ByteStreams.copy(in, out);
                        }
                      }
                      return compressed;
                    }
                    return uncompressed;
                  }

                  @Override public void processElement(ProcessContext context) throws IOException {
                    StorageObject object = context.sideInput(objectTag);
                    String contentType = Optional
                        .fromNullable(object.getContentType())
                        .or("application/octet-stream");
                    context.output(context
                        .sideInput(apiFactoryTag)
                        .createApi(StorageApiFactoryImplementation.INSTANCE)
                        .objects()
                        .insert(
                            object.getBucket(),
                            object,
                            new FileContent(
                                contentType,
                                compress(
                                    Compression.forContentType(contentType),
                                    context.element())))
                        .execute());
                  }
                }))
        .setCoder(GenericJsonCoder.of(StorageObject.class))
        .apply(Convert.<StorageObject>fromSingleton());
  }
}