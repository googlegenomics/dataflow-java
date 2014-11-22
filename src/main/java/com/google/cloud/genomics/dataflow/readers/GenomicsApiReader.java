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
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.genomics.utils.GenomicsFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;

public abstract class GenomicsApiReader<I extends GenericJson, O extends GenericJson> 
    extends DoFn<I, O> {
  // Used for access to the genomics API
  protected final GenomicsFactory.OfflineAuth auth;
  protected final String fields;

  public GenomicsApiReader(GenomicsFactory.OfflineAuth auth, String fields) {
    this.auth = auth;
    this.fields = fields;
  }

  @Override
  public void processElement(ProcessContext c) {
    try {
      processApiCall(auth.getGenomics(), c, c.element());
    } catch (IOException | GeneralSecurityException e) {
      throw new RuntimeException(
          "Failed to create genomics API request - this shouldn't happen.", e);
    }
  }

  protected abstract void processApiCall(Genomics genomics, ProcessContext c, I element)
      throws IOException;
}
