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

import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.Genomics.Variants.Search;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.genomics.utils.Paginator;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

public class VariantReader extends GenomicsApiReader<SearchVariantsRequest, Variant> {
  private static final Logger LOG = Logger.getLogger(VariantReader.class.getName());
  private int numRetries;
  private final String variantFields;

  public VariantReader(String applicationName, String apiKey, 
      File clientSecretsFile, String variantFields) {
    super(applicationName, apiKey, clientSecretsFile);
    this.variantFields = variantFields;
  }
  
  public VariantReader(String applicationName, File clientSecretsFile, String variantFields) {
    this(applicationName, null, clientSecretsFile, variantFields);
  }
  
  public VariantReader(String applicationName, String apiKey, String variantFields) {
    this(applicationName, apiKey, null, variantFields);
  }
  
  /**
   * Sets the number of times to retry requests. If 0, will never retry. If -1, will always retry.
   * @param numRetries Number of times to retry requests. Set to 0 for never or -1 for always.
   */
  public void setRetries(int numRetries) {
    this.numRetries = numRetries;
  }

  @Override
  protected void processApiCall(Genomics genomics, ProcessContext c, SearchVariantsRequest request)
      throws IOException {
    LOG.info("Starting Variants read loop");
    
    Paginator.Variants searchVariants;
    
    switch (numRetries) {
      case -1:  searchVariants = 
          Paginator.Variants.create(genomics, Paginator.alwaysRetry()); break;
      case 0:   searchVariants = 
          Paginator.Variants.create(genomics, Paginator.neverRetry()); break;
      default:  searchVariants = 
          Paginator.Variants.create(genomics, Paginator.retryNTimes(numRetries));
    }
    
    long total = 0;
    for (Variant read : searchVariants.search(request, 
        new Paginator.GenomicsRequestInitializer<Genomics.Variants.Search>() {
      @Override
      public void initialize(Search search) {
        if (variantFields != null) {
          search.setFields(variantFields);
        }
      }})) {
      c.output(read);
      total++;
    }
    
    LOG.info("Read " + total + " variants at: " + 
        request.getContig() + "-" + request.getStartPosition());
  }
}