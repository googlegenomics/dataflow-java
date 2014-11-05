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

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.cloud.dataflow.sdk.transforms.Convert;
import com.google.cloud.dataflow.sdk.transforms.CreatePObject;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.dataflow.sdk.values.PObjectTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.utils.Paginator;

import java.io.IOException;

public class Genomics {

  private static final ApiFactory.Implementation<com.google.api.services.genomics.Genomics>
      GENOMICS_IMPL =
      new ApiFactory.Implementation<com.google.api.services.genomics.Genomics>() {
        @Override public com.google.api.services.genomics.Genomics createClient(
            HttpTransport transport,
            JsonFactory jsonFactory,
            HttpRequestInitializer httpRequestInitializer,
            String appName) {
          return new com.google.api.services.genomics.Genomics
              .Builder(transport, jsonFactory, httpRequestInitializer)
              .setApplicationName(appName)
              .build();
        }
      };

  public static Genomics of(PObject<ApiFactory> apiFactory) {
    return new Genomics(apiFactory);
  }

  private final PObject<ApiFactory> apiFactory;

  private Genomics(PObject<ApiFactory> apiFactory) {
    this.apiFactory = apiFactory;
  }

  public PCollection<Read> searchReads(SearchReadsRequest request) {
    return searchReads(apiFactory.getPipeline().apply(CreatePObject.of(request))
        .setCoder(GenericJsonCoder.of(SearchReadsRequest.class)));
  }

  public PCollection<Read> searchReads(PObject<SearchReadsRequest> request) {
    final TupleTag<SearchReadsRequest> requestTag = new TupleTag<>();
    return apiFactory
        .apply(Convert.<ApiFactory>toSingleton())
        .apply(ParDo
            .withSideInputs(PObjectTuple.of(requestTag, request))
            .of(
                new DoFn<ApiFactory, Read>() {
                  @Override public void processElement(final ProcessContext context)
                      throws IOException {
                    Paginator.READS
                        .createPaginator(context.element().createApi(GENOMICS_IMPL))
                        .search(
                            context.sideInput(requestTag),
                            new Paginator.Callback<Read, Void>() {
                              @Override public Void consumeResponses(Iterable<Read> responses)
                                  throws IOException {
                                for (Read read : responses) {
                                  context.output(read);
                                }
                                return null;
                              }
                            });
                  }
                }));
  }
}