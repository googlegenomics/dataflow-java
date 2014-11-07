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

import com.google.api.client.http.FileContent;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.transforms.Convert;
import com.google.cloud.dataflow.sdk.transforms.CreatePObject;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.dataflow.sdk.values.PObjectTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Storage implements Serializable {

  private enum Compression {

    GZIP {

      @Override Optional<String> contentType() {
        return Optional.of("application/gzip");
      }

      @Override InputStream wrap(InputStream in) throws IOException {
        return new GZIPInputStream(in);
      }

      @Override OutputStream wrap(OutputStream out) throws IOException {
        return new GZIPOutputStream(out);
      }
    },

    UNCOMPRESSED {

      @Override Optional<String> contentType() {
        return Optional.absent();
      }

      @Override InputStream wrap(InputStream in) {
        return in;
      }

      @Override OutputStream wrap(final OutputStream out) {
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

    static Compression forContentType(String contentType) {
      return Optional
          .fromNullable(COMPRESSION_BY_CONTENT_TYPE.get(Optional.fromNullable(contentType)))
          .or(UNCOMPRESSED);
    }

    abstract Optional<String> contentType();

    abstract InputStream wrap(InputStream in) throws IOException;

    abstract OutputStream wrap(OutputStream out) throws IOException;
  }

  public interface Deserializer<T> extends Serializable {

    Iterable<T> deserialize(InputStream in) throws IOException;
  }

  public interface Serializer<T> extends Serializable {

    void serialize(T object, OutputStream out) throws IOException;
  }

  private static final ApiFactory.Implementation<com.google.api.services.storage.Storage>
      STORAGE_IMPL =
      new ApiFactory.Implementation<com.google.api.services.storage.Storage>() {
        @Override public com.google.api.services.storage.Storage createClient(
            HttpTransport transport,
            JsonFactory jsonFactory,
            HttpRequestInitializer httpRequestInitializer,
            String appName) {
          return new com.google.api.services.storage.Storage
              .Builder(transport, jsonFactory, httpRequestInitializer)
              .setApplicationName(appName)
              .build();
        }
      };

  public static Storage of(PObject<ApiFactory> apiFactory) {
    return new Storage(apiFactory);
  }

  private transient final PObject<ApiFactory> apiFactory;

  private Storage(PObject<ApiFactory> apiFactory) {
    this.apiFactory = apiFactory;
  }

  public PObject<StorageObject> get(PObject<String> bucket, PObject<String> name) {
    final TupleTag<String> bucketTag = new TupleTag<>();
    final TupleTag<String> nameTag = new TupleTag<>();
    return apiFactory
        .apply(Convert.<ApiFactory>toSingleton())
        .apply(ParDo
            .withSideInputs(PObjectTuple.of(bucketTag, bucket).and(nameTag, name))
            .of(
                new DoFn<ApiFactory, StorageObject>() {
                  @Override public void processElement(ProcessContext context) throws IOException {
                    context.output(context
                        .element()
                        .createApi(STORAGE_IMPL)
                        .objects()
                        .get(context.sideInput(bucketTag), context.sideInput(nameTag))
                        .execute());
                  }
                }))
        .setCoder(GenericJsonCoder.of(StorageObject.class))
        .apply(Convert.<StorageObject>fromSingleton());
  }

  public PObject<StorageObject> get(String bucket, String name) {
    return get(
        pipeline().apply(CreatePObject.of(bucket)).setCoder(StringUtf8Coder.of()),
        pipeline().apply(CreatePObject.of(name)).setCoder(StringUtf8Coder.of()));
  }

  public PObject<ApiFactory> apiFactory() {
    return apiFactory;
  }

  public Pipeline pipeline() {
    return apiFactory().getPipeline();
  }

  @SuppressWarnings("unchecked")
  public <T> PCollection<T> read(PObject<StorageObject> object, Deserializer<T> deserializer) {
    return read(
        object,
        pipeline()
            .apply(CreatePObject.of(deserializer))
            .setCoder(SerializableCoder.of((Class<Deserializer<T>>) (Object) Deserializer.class)));
  }

  public <T> PCollection<T> read(
      PObject<StorageObject> object,
      PObject<Deserializer<T>> deserializer) {
    final TupleTag<StorageObject> objectTag = new TupleTag<>();
    final TupleTag<Deserializer<T>> deserializerTag = new TupleTag<>();
    return apiFactory
        .apply(Convert.<ApiFactory>toSingleton())
        .apply(ParDo
            .withSideInputs(PObjectTuple.of(objectTag, object).and(deserializerTag, deserializer))
            .of(
                new DoFn<ApiFactory, T>() {
                  @Override public void processElement(ProcessContext context) throws IOException {
                    StorageObject object = context.sideInput(objectTag);
                    try (InputStream in = Compression
                        .forContentType(Optional
                            .fromNullable(object.getContentType())
                            .or("application/octet-stream"))
                        .wrap(context
                            .element()
                            .createApi(STORAGE_IMPL)
                            .objects()
                            .get(object.getBucket(), object.getName())
                            .executeMediaAsInputStream())) {
                      for (T t : context.sideInput(deserializerTag).deserialize(in)) {
                        context.output(t);
                      }
                    }
                  }
                }));
  }

  public <T> PCollection<T> read(StorageObject object, Deserializer<T> deserializer) {
    return read(pObject(object), deserializer);
  }

  public <T> PObject<StorageObject> write(
      PCollection<? extends T> collection,
      PObject<StorageObject> object,
      PObject<Serializer<T>> serializer) {
    final TupleTag<ApiFactory> apiFactoryTag = new TupleTag<>();
    final TupleTag<StorageObject> objectTag = new TupleTag<>();
    final TupleTag<Serializer<T>> serializerTag = new TupleTag<>();
    return collection
        .apply(ParDo
            .withSideInputs(PObjectTuple
                .of(apiFactoryTag, apiFactory)
                .and(objectTag, object)
                .and(serializerTag, serializer))
            .of(
                new DoFn<T, StorageObject>() {

                  private String contentType;
                  private File file;
                  private StorageObject object;
                  private OutputStream out;
                  private Serializer<T> serializer;

                  @Override public void finishBatch(Context context) throws IOException {
                    out.close();
                    context.output(context
                        .sideInput(apiFactoryTag)
                        .createApi(STORAGE_IMPL)
                        .objects()
                        .insert(object.getBucket(), object, new FileContent(contentType, file))
                        .execute());
                  }

                  @Override public void processElement(ProcessContext context) throws IOException {
                    serializer.serialize(context.element(), out);
                  }

                  @Override public void startBatch(Context context) throws IOException {
                    out = Compression
                        .forContentType(contentType = Optional
                            .fromNullable((object = context.sideInput(objectTag)).getContentType())
                            .or("application/octet-stream"))
                        .wrap(new FileOutputStream(
                            file = File.createTempFile(object.getName(), ".tmp")));
                    file.deleteOnExit();
                    serializer = context.sideInput(serializerTag);
                  }
                }))
        .setCoder(GenericJsonCoder.of(StorageObject.class))
        .apply(Convert.<StorageObject>fromSingleton());
  }

  @SuppressWarnings("unchecked")
  public <T> PObject<StorageObject> write(
      PCollection<? extends T> collection,
      PObject<StorageObject> object,
      Serializer<T> serializer) {
    return write(
        collection,
        object,
        pipeline()
            .apply(CreatePObject.of(serializer))
            .setCoder(SerializableCoder.of((Class<Serializer<T>>) (Object) Serializer.class)));
  }

  public <T> PObject<StorageObject> write(
      PCollection<? extends T> collection,
      StorageObject object,
      Serializer<T> serializer) {
    return write(collection, pObject(object), serializer);
  }

  public PObject<StorageObject> pObject(StorageObject object) {
    return apiFactory()
        .getPipeline()
        .apply(CreatePObject.of(object))
        .setCoder(GenericJsonCoder.of(StorageObject.class));
  }
}