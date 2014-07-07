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

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.genomics.dataflow.GenomicsApi;

import java.io.IOException;

public abstract class GenomicsApiReader<I, O> extends DoFn<I, O> {
  // Used for access to the genomics API
  // If the accessToken is null, then an apiKey is required
  protected final String accessToken;
  protected final String apiKey;

  public GenomicsApiReader(String accessToken, String apiKey) {
    this.accessToken = accessToken;
    this.apiKey = apiKey;
  }

  @Override
  public void processElement(ProcessContext c) {
    try {
      processApiCall(new GenomicsApi(accessToken, apiKey), c, c.element());
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create genomics API request - this shouldn't happen.", e);
    }
  }

  protected abstract void processApiCall(GenomicsApi api, ProcessContext c, I element)
      throws IOException;
}
