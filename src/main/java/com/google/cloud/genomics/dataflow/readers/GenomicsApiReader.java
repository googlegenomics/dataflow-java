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
import com.google.cloud.genomics.dataflow.utils.GenomicsAuth;
import com.google.cloud.genomics.utils.Paginator;
import com.google.common.base.Supplier;

import java.io.IOException;
import java.security.GeneralSecurityException;

public abstract class GenomicsApiReader<I extends GenericJson, O extends GenericJson> 
    extends DoFn<I, O> {
  // Used for access to the genomics API
  // If the clientSecretsFile is null, then an apiKey is required
  protected final GenomicsAuth auth;
  protected final String fields;
  protected int numRetries = 10;
  
  public GenomicsApiReader(GenomicsAuth auth, String fields) {
    this.auth = auth;
    this.fields = fields;
  }
  
  /**
   * Sets the number of times to retry requests. If 0, will never retry. If -1, will always retry.
   * @param numRetries Number of times to retry requests. Set to 0 for never or -1 for always.
   */
  public void setRetries(int numRetries) {
    this.numRetries = numRetries;
  }
  
  /**
   * Returns the retry policy for this reader based on its numRetries field
   * @return the retry policy for this reader
   */
  public Supplier<Paginator.RetryPolicy<I>> getRetryPolicy() {
    switch (numRetries) {
      case -1:  return Paginator.alwaysRetry();
      case 0:   return Paginator.neverRetry();
      default:  return Paginator.retryNTimes(numRetries);
    }
  }

  @Override
  public void processElement(ProcessContext c) {
    try {
      processApiCall(auth.getService(), c, c.element());
    } catch (IOException | GeneralSecurityException e) {
      throw new RuntimeException(
          "Failed to create genomics API request - this shouldn't happen.", e);
    }
  }

  protected abstract void processApiCall(Genomics genomics, ProcessContext c, I element)
      throws IOException;
}
