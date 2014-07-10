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
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.GenomicsApi;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Converts Readsets to Key values of Readset name and Read bases
 * Input: Readset
 * Output: KV(Name, Read Bases)
 */
public class ReadReader extends GenomicsApiReader<Readset, KV<String, String>> {
  private String readFields;
  
  public ReadReader(String accessToken, String apiKey, String readFields) {
    super(accessToken, apiKey);
    this.readFields = readFields;
  }

  @Override
  protected void processApiCall(GenomicsApi api, ProcessContext c, Readset set) throws IOException {
    SearchReadsRequest request = new SearchReadsRequest()
        .setReadsetIds(ImmutableList.of(set.getId()))
        .setMaxResults(new BigInteger("1024"));

    do {
      SearchReadsResponse response = api.executeRequest(
          api.getService().reads().search(request), readFields);

      for (Read read : response.getReads()) {
        c.output(KV.of(set.getName(), read.getOriginalBases()));
      }
      request.setPageToken(response.getNextPageToken());
    } while (request.getPageToken() != null);
  }
}
