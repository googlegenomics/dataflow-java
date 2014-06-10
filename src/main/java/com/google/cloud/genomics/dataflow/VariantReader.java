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
package com.google.cloud.genomics.dataflow;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.api.services.genomics.model.SearchVariantsResponse;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.Serializable;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.logging.Logger;

// TODO: Turn this into a real dataflow reader
public class VariantReader {
  private static final Logger LOG = Logger.getLogger(VariantReader.class.getName());
  private static final int API_RETRIES = 3;

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

  public static class GetVariants extends DoFn<Options, Variant> {
    @Override
    public void processElement(ProcessContext c) {
      VariantReader reader = new VariantReader(c.element());
      for (Variant variant : reader.getVariants()) {
        c.output(variant);
      }
    }
  }

  private final Genomics service;
  private final Options options;

  public VariantReader(Options options) {
    this.options = options;

    GoogleCredential credential = options.accessToken == null ? null :
        new GoogleCredential().setAccessToken(options.accessToken);
    try {
      service = new Genomics.Builder(GoogleNetHttpTransport.newTrustedTransport(), new JacksonFactory(), credential)
          .setApplicationName("dataflow-reader").build();
    } catch (GeneralSecurityException | IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public List<Variant> getVariants() {
    SearchVariantsResponse response = getVariantsResponse(options, null);
    List<Variant> variants = response.getVariants();
    if (variants == null) {
      variants = Lists.newArrayList();
    }

    String nextPageToken = response.getNextPageToken();
    while (nextPageToken != null) {
      response = getVariantsResponse(options, nextPageToken);
      variants.addAll(response.getVariants());
      nextPageToken = response.getNextPageToken();
    }

    LOG.info("Got " + variants.size() + " variants at: " + options.contig + "-" + options.start);
    return variants;
  }

  private SearchVariantsResponse getVariantsResponse(Options options, String nextPageToken) {
    SearchVariantsRequest request = new SearchVariantsRequest()
        .setDatasetId(options.datasetId)
        .setContig(options.contig)
        .setStartPosition(options.start)
        .setEndPosition(options.end);

    if (nextPageToken != null) {
      request.setPageToken(nextPageToken);
    }

    for (int i = 0; i < API_RETRIES; i++) {
      try {
        return service.variants().search(request)
            .setFields(options.variantFields)
            .setKey(options.apiKey).execute();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    throw new RuntimeException("Genomics API call failed multiple times in a row.");
  }
}
