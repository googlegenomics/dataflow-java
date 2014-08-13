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
import com.google.api.services.genomics.Genomics.Reads.Search;
import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.cloud.genomics.utils.Paginator;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

public class ReadReader extends GenomicsApiReader<SearchReadsRequest, Read> {
  private static final Logger LOG = Logger.getLogger(ReadReader.class.getName());
  private int numRetries = 10;
  private String readFields;
  
  public ReadReader(String applicationName, String apiKey, 
      File clientSecretsFile, String readFields) {
    super(applicationName, apiKey, clientSecretsFile);
    this.readFields = readFields;
  }
  
  public ReadReader(String applicationName, File clientSecretsFile, String readFields) {
    this(applicationName, null, clientSecretsFile, readFields);
  }
  
  public ReadReader(String applicationName, String apiKey, String readFields) {
    this(applicationName, apiKey, null, readFields);
  }
  
  /**
   * Sets the number of times to retry requests. If 0, will never retry. If -1, will always retry.
   * @param numRetries Number of times to retry requests. Set to 0 for never or -1 for always.
   */
  public void setRetries(int numRetries) {
    this.numRetries = numRetries;
  }

  @Override
  protected void processApiCall(Genomics genomics, ProcessContext c, SearchReadsRequest request)
      throws IOException {
    LOG.info("Starting Reads read loop");
    
    Paginator.Reads searchReads;
    
    switch (numRetries) {
      case -1:  searchReads = Paginator.Reads.create(genomics, Paginator.alwaysRetry()); break;
      case 0:   searchReads = Paginator.Reads.create(genomics, Paginator.neverRetry()); break;
      default:  searchReads = Paginator.Reads.create(genomics, Paginator.retryNTimes(numRetries));
    }
    
    long total = 0;
    for (Read read : searchReads.search(request, 
        new Paginator.GenomicsRequestInitializer<Genomics.Reads.Search>() {
      @Override
      public void initialize(Search search) {
        if (readFields != null) {
          search.setFields(readFields);
        }
      }})) {
      c.output(read);
      total++;
    }
    
    LOG.info("Read " + total + " reads");
  }
}
