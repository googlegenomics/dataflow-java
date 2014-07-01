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
import com.google.common.collect.ImmutableList;

import java.io.IOException;

/**
 * Converts Readsets to Key values of Readset name and Read bases
 * Input: Readset
 * Output: KV(Name, Read Bases)
 */
public class ReadsetToReads extends DoFn<Readset, KV<String, String>> {
  private String accessToken;
  private String apiKey;
  private String readFields;
  
  public ReadsetToReads(String accessToken, String apiKey, String readFields) {
    this.accessToken = accessToken;
    this.apiKey = apiKey;
    this.readFields = readFields;
  }
  
  @Override
  public void processElement(ProcessContext c) {
    GenomicsApi api = new GenomicsApi(accessToken, apiKey);

    Readset set = c.element();
    SearchReadsRequest request = new SearchReadsRequest()
        .setReadsetIds(ImmutableList.of(set.getId()));

    do {
      SearchReadsResponse response;
      try {
        response = api.executeRequest(api.getService().reads().search(request), readFields);
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to create genomics API request - this shouldn't happen.", e);
      }

      for (Read read : response.getReads()) {
        c.output(KV.of(set.getName(), read.getOriginalBases()));
      }
      request.setPageToken(response.getNextPageToken());
    } while (request.getPageToken() != null);
  }
}
