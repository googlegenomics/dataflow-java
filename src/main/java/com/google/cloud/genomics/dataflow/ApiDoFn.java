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

import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.genomics.utils.JsonClientFactory;
import com.google.common.collect.FluentIterable;

import java.util.Arrays;
import java.util.Collections;

/**
 * An abstract superclass for creating {@link DoFn}s that invoke Google APIs
 */
public abstract class ApiDoFn<C extends AbstractGoogleJsonClient,
    B extends AbstractGoogleJsonClient.Builder, I, O> extends DoFn<I, O> {

  private final JsonClientFactory<? extends C, B> clientFactory;
  private C client;

  protected ApiDoFn(JsonClientFactory.Logic<? extends C, B> logic) {
    this.clientFactory = JsonClientFactory.create(logic);
  }

  protected Iterable<String> additionalScopes() {
    return Collections.emptyList();
  }

  @SuppressWarnings("unused")
  protected void finishBatch(C client, Context context) throws Exception {
  }

  @Override public final void finishBatch(Context context) throws Exception {
    finishBatch(client, context);
  }

  protected abstract void processElement(C client, ProcessContext context) throws Exception;

  @Override public final void processElement(ProcessContext context) throws Exception {
    processElement(client, context);
  }

  @SuppressWarnings("unused")
  protected void startBatch(C client, Context context) throws Exception {
  }

  @Override public final void startBatch(Context context) throws Exception {
    startBatch(
        client = clientFactory.create(FluentIterable
            .from(Arrays.asList(
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/devstorage.full_control",
                "https://www.googleapis.com/auth/userinfo.email",
                "https://www.googleapis.com/auth/datastore"))
            .append(additionalScopes())
            .toSet()),
        context);
  }
}