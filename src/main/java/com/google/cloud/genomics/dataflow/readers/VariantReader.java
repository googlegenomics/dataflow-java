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

import com.google.api.services.genomics.GenomicsRequest;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.api.services.genomics.model.SearchVariantsResponse;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.genomics.dataflow.GenomicsApi;

import java.io.IOException;
import java.io.Serializable;
import java.util.logging.Logger;

public class VariantReader extends DoFn<VariantReader.Options, Variant> {
  private static final Logger LOG = Logger.getLogger(VariantReader.class.getName());

  public static class Options implements Serializable {
    // Used for access to the genomics API
    // If the accessToken is null, then an apiKey is required
    private final String apiKey;
    private final String accessToken;

    // Specific to the variants search request
    private final String datasetId;
    private final String variantFields;
    private final String contig;
    private final long start;
    private final long end;

    public Options(String apiKey, String accessToken, String datasetId, String variantFields,
                   String contig, long start, long end) {
      this.apiKey = apiKey;
      this.accessToken = accessToken;
      this.datasetId = datasetId;
      this.variantFields = variantFields;
      this.contig = contig;
      this.start = start;
      this.end = end;
    }
  }

  @Override
  public void processElement(ProcessContext c) {
    Options options = c.element();
    GenomicsApi api = new GenomicsApi(options.accessToken, options.apiKey);

    String nextPageToken = null;
    do {
      SearchVariantsResponse response = getVariantsResponse(api, options, nextPageToken);
      if (response.getVariants() == null) {
        break;
      }
      for (Variant variant : response.getVariants()) {
        c.output(variant);
      }
      nextPageToken = response.getNextPageToken();
    } while (nextPageToken != null);

    LOG.info("Finished variants at: " + options.contig + "-" + options.start);
  }

  private SearchVariantsResponse getVariantsResponse(GenomicsApi api, Options options,
      String nextPageToken) {
    SearchVariantsRequest request = new SearchVariantsRequest()
        .setDatasetId(options.datasetId)
        .setContig(options.contig)
        .setStartPosition(options.start)
        .setEndPosition(options.end);

    if (nextPageToken != null) {
      request.setPageToken(nextPageToken);
    }

    try {
      GenomicsRequest<SearchVariantsResponse> search = api.getService().variants().search(request);
      return api.executeRequest(search, options.variantFields);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create genomics API request - this shouldn't happen.", e);
    }
  }
}
