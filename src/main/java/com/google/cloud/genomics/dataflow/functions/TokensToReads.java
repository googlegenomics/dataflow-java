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
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.api.services.genomics.model.SearchReadsResponse;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.GenomicsApi;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * Converts Readsets to Key values of Readset name and Read
 * Input: KV(Readset, Page Token)
 * Output: Set<KV(Name, Read Bases)>
 */
public class TokensToReads extends DoFn<KV<String, String>, KV<String, String>> {
  private static final String END_TOKEN = "KMER_INDEX_PIPELINE_END_READ_QUERY";
  private String accessToken;
  private String apiKey;
  private String readFields;
  
  public TokensToReads(String accessToken, String apiKey) {
    this(accessToken, apiKey, null);
  }
  
  public TokensToReads(String accessToken, String apiKey, String readFields) {
    this.accessToken = accessToken;
    this.apiKey = apiKey;
    this.readFields = readFields;
  }
  
  @Override
  public void processElement(ProcessContext c) {
    GenomicsApi api = new GenomicsApi(accessToken, apiKey);

    KV<String, String> elem = c.element();
    String[] dat = elem.getKey().split("\t");
    String name = dat[0];
    String id = dat[1];
    String token = elem.getValue();
    if(token.equals(ReadsetToTokens.NULL_TOKEN)) {
      token = null;
    }
    
    SearchReadsRequest request = new SearchReadsRequest()
        .setReadsetIds(ImmutableList.of(id)).setPageToken(token);
    
    List<Read> reads = getReads(request, api);
    for (Read read : reads) {
      c.output(KV.of(name, read.getOriginalBases()));
    }
  }
  
  @VisibleForTesting
  List<Read> getReads(SearchReadsRequest request, GenomicsApi api) {
    // Use page token to see when requests should end
    if (request.getPageToken() != null && request.getPageToken().equals(END_TOKEN)) {
      return null;
    }
    
    List<Read> result = Lists.newArrayList();
    SearchReadsResponse response;
    try {
      response = api.executeRequest(api.getService().reads().search(request), readFields);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create genomics API request - this shouldn't happen.", e);
    }
    
    result.addAll(response.getReads());

    if (response.getNextPageToken() != null) {
      request.setPageToken(response.getNextPageToken());
    } else {
      request.setPageToken(END_TOKEN);
    }
    return result;
  }
}
