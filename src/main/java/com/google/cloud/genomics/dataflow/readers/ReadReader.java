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

import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.api.services.genomics.model.SearchReadsResponse;
import com.google.cloud.genomics.dataflow.utils.GenomicsApi;

import java.io.IOException;
import java.util.logging.Logger;

public class ReadReader extends GenomicsApiReader<SearchReadsRequest, Read> {
  private static final Logger LOG = Logger.getLogger(ReadReader.class.getName());
  private String readFields;
  
  public ReadReader(String accessToken, String apiKey, String readFields) {
    super(accessToken, apiKey);
    this.readFields = readFields;
  }

  @Override
  protected void processApiCall(GenomicsApi api, ProcessContext c, SearchReadsRequest request)
      throws IOException {
    long total = 0;
    LOG.info("Starting read loop");
    do {
      SearchReadsResponse response = api.executeRequest(
          api.getService().reads().search(request), readFields);

      if (response.getReads() == null) {
        break;
      }
      
      for (Read read : response.getReads()) {
        c.output(read);
      }
      
      total += response.getReads().size();
      LOG.info("Read " + total + " reads");
      request.setPageToken(response.getNextPageToken());
    } while (request.getPageToken() != null);
  }
}
