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

import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.api.services.genomics.model.SearchVariantsResponse;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.genomics.dataflow.utils.GenomicsApi;

import java.io.IOException;
import java.util.logging.Logger;

public class VariantReader extends GenomicsApiReader<SearchVariantsRequest, Variant> {
  private static final Logger LOG = Logger.getLogger(VariantReader.class.getName());

  private final String variantFields;

  public VariantReader(String accessToken, String apiKey, String variantFields) {
    super(accessToken, apiKey);
    this.variantFields = variantFields;
  }

  @Override
  protected void processApiCall(GenomicsApi api, ProcessContext c, SearchVariantsRequest request)
      throws IOException {

    do {
      SearchVariantsResponse response = api.executeRequest(
          api.getService().variants().search(request), variantFields);

      if (response.getVariants() == null) {
        break;
      }
      for (Variant variant : response.getVariants()) {
        c.output(variant);
      }
      request.setPageToken(response.getNextPageToken());
    } while (request.getPageToken() != null);

    LOG.info("Finished variants at: " + request.getContig() + "-" + request.getStartPosition());
  }
}
