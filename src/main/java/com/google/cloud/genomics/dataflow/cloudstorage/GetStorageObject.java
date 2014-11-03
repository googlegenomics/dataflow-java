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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class GetStorageObject extends PTransform<GetStorageObject.Input, PObject<StorageObject>> {

  public static class Input extends AbstractPInput<Input> {

    public static Input of(
        PObject<ApiFactory> apiFactory,
        PObject<String> bucket,
        PObject<String> name) {
      return new Input(apiFactory, bucket, name);
    }

    private final PObject<ApiFactory> apiFactory;
    private final PObject<String> bucket;
    private final PObject<String> name;

    private Input(PObject<ApiFactory> apiFactory, PObject<String> bucket, PObject<String> name) {
      this.apiFactory = apiFactory;
      this.bucket = bucket;
      this.name = name;
    }

    public PObject<ApiFactory> apiFactory() {
      return apiFactory;
    }

    public PObject<String> bucket() {
      return bucket;
    }

    @Override public Collection<? extends PValue> expand() {
      return Arrays.asList(apiFactory(), bucket(), name());
    }

    public PObject<String> name() {
      return name;
    }
  }

  public static GetStorageObject of() {
    return new GetStorageObject();
  }

  private final TupleTag<ApiFactory> apiFactoryTag = new TupleTag<>();
  private final TupleTag<String> bucketTag = new TupleTag<>();

  private GetStorageObject() {}

  @Override public PObject<StorageObject> apply(Input input) {
    return input
        .name()
        .apply(Convert.<String>toSingleton())
        .apply(ParDo
            .withSideInputs(PObjectTuple
                .of(apiFactoryTag, input.apiFactory())
                .and(bucketTag, input.bucket()))
            .of(
                new DoFn<String, StorageObject>() {
                  @Override public void processElement(ProcessContext context) throws IOException {
                    context.output(context
                        .sideInput(apiFactoryTag)
                        .createApi(StorageApiFactoryImplementation.INSTANCE)
                        .objects()
                        .get(context.sideInput(bucketTag), context.element())
                        .execute());
                  }
                }))
        .setCoder(GenericJsonCoder.of(StorageObject.class))
        .apply(Convert.<StorageObject>fromSingleton());
  }
}