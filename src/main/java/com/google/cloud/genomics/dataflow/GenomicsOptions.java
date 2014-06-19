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

import com.google.cloud.dataflow.sdk.runners.Description;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Contains common genomics pipeline options.
 * Extend this class to add additional command line args.
 */
public class GenomicsOptions extends PipelineOptions {
  @Description("The ID of the Google Genomics dataset this pipeline is working with. " +
      "Defaults to 1000 Genomes.")
  public String datasetId = "376902546192";

  @Description("If querying a public dataset, provide a Google API key that has access " +
      "to genomics data and no OAuth will be performed.")
  public String apiKey = null;

  @Description("If querying a private dataset, or performing any write operations, " +
      "you need to provide the path to client_secrets.json. Do not supply an api key.")
  public String clientSecretsFilename = "client_secrets.json";

  // Gets the access token to use for this pipeline
  // If there is an apiKey, this will always return null
  public String getAccessToken() {
    if (apiKey == null) {
      return null;
    }

    try {
      return new OAuthHelper().getAccessToken(clientSecretsFilename,
            Lists.newArrayList(OAuthHelper.GENOMICS_SCOPE));
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException("Could not fetch the OAuth access token - please try again", e);
    }
  }
}
