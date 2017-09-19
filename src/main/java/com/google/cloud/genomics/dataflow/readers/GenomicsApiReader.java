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
package com.google.cloud.genomics.dataflow.readers;

import com.google.api.client.json.GenericJson;
import com.google.api.services.genomics.Genomics;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Sum;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.OfflineAuth;

import java.util.logging.Logger;

public abstract class GenomicsApiReader<I extends GenericJson, O extends GenericJson>
    extends DoFn<I, O> {
  private static final Logger LOG = Logger.getLogger(GenomicsApiReader.class.getName());

  // Used for access to the genomics API
  protected final OfflineAuth auth;
  protected final String fields;

  public GenomicsApiReader(OfflineAuth auth, String fields) {
    this.auth = auth;
    this.fields = fields;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    GenomicsFactory factory = GenomicsFactory.builder().build();
    Genomics genomics = factory.fromOfflineAuth(auth);

    processApiCall(genomics, c, c.element());

    Metrics.counter(GenomicsApiReader.class, "Genomics API Initialized Request Count")
        .inc(factory.initializedRequestsCount());
    Metrics.counter(GenomicsApiReader.class, "Genomics API Unsuccessful Response Count")
        .inc(factory.unsuccessfulResponsesCount());
    Metrics.counter(GenomicsApiReader.class, "Genomics API IOException Response Count")
        .inc(factory.ioExceptionsCount());
    LOG.info("ApiReader processed " + factory.initializedRequestsCount() + " requests ("
        + factory.unsuccessfulResponsesCount() + " server errors and "
        + factory.ioExceptionsCount() + " IO exceptions)");
  }

  protected abstract void processApiCall(Genomics genomics, ProcessContext c, I element);
}
