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
import com.google.api.services.genomics.model.Readset;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.api.services.genomics.model.SearchReadsResponse;
import com.google.cloud.genomics.dataflow.utils.GenomicsApi;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.math.BigInteger;
import java.util.logging.Logger;

/**
 * Takes in a readset and returns all the reads under that readset
 */
public class ReadsetToReads extends GenomicsApiReader<Readset, Read> {
  private static final Logger LOG = Logger.getLogger(ReadsetToReads.class.getName());
  private String readFields;

  public ReadsetToReads(String accessToken, String apiKey, String readFields) {
    super(accessToken, apiKey);
    this.readFields = readFields;
  }

  @Override
  protected void processApiCall(GenomicsApi api, ProcessContext c, Readset set) throws IOException {
    SearchReadsRequest request = new SearchReadsRequest()
        .setReadsetIds(ImmutableList.of(set.getId()))
        .setMaxResults(new BigInteger("1024"));

    long total = 0;
    do {
      SearchReadsResponse response = api.executeRequest(
          api.getService().reads().search(request), readFields);
      total += response.getReads().size();
      
      for (Read read : response.getReads()) {
        c.output(read);
      }
      request.setPageToken(response.getNextPageToken());
      
      LOG.info("Loaded " + total + " reads for readset " + set.getId());
    } while (request.getPageToken() != null);
  }
}
