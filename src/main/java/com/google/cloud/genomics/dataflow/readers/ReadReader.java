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
import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.cloud.genomics.utils.Paginator;

import java.io.IOException;
import java.util.logging.Logger;

public class ReadReader extends GenomicsApiReader<SearchReadsRequest, Read> {
  private static final Logger LOG = Logger.getLogger(ReadReader.class.getName());
  
  public ReadReader(String applicationName, String apiKey, 
      String accessToken, String readFields) {
    super(applicationName, apiKey, accessToken, readFields);
  }
  
  @Override
  protected void processApiCall(Genomics genomics, ProcessContext c, SearchReadsRequest request)
      throws IOException {
    LOG.info("Starting Reads read loop");
    
    Paginator.Reads searchReads = Paginator.Reads.create(genomics, getRetryPolicy());
    
    for (Read read : searchReads.search(request, fields)) {
      c.output(read);
    }
  }
}
