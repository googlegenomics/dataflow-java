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

package com.google.cloud.genomics.dataflow.functions;

import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.Readset;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.api.services.genomics.model.SearchReadsResponse;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.GenomicsApi;
import com.google.cloud.genomics.dataflow.GenomicsOptions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * Converts Readsets to Key values of Readset name and Read
 */
public class ReadsetToRead extends DoFn<Readset, KV<String, Read>> {
  private static final String READ_FIELDS = "nextPageToken,reads(originalBases)";
  private final GenomicsOptions options;
  
  public ReadsetToRead(GenomicsOptions options) {
    this.options = options;
  }
  
  @Override
  public void processElement(ProcessContext c) {
    GenomicsApi api = new GenomicsApi(options.getAccessToken(), options.apiKey);

    Readset set = c.element();
    SearchReadsRequest request = new SearchReadsRequest()
        .setReadsetIds(Lists.newArrayList(set.getId()));
    List<Read> reads;
    while ((reads = getReads(request, api)) != null && reads.size() < 500) {
      for (Read read : reads) {
        c.output(KV.of(set.getName(), read));
      }
    }
  }
  
  public List<Read> getReads(SearchReadsRequest request, GenomicsApi api) {
    // Bit of pointer logic to iterate through pages
    if (request == null) {
      return null;
    }
    
    List<Read> result = Lists.newArrayList();
    SearchReadsResponse response;
    try {
      response = api.executeRequest(api.getService().reads().search(request), READ_FIELDS);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create genomics API request - this shouldn't happen.", e);
    }
    
    result.addAll(response.getReads());

    if (response.getNextPageToken() != null) {
      request.setPageToken(response.getNextPageToken());
    } else {
      request = null;
    }
    return result;
  }
}
