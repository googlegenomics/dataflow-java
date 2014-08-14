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
package com.google.cloud.genomics.dataflow.utils;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.genomics.Genomics;
import com.google.cloud.genomics.utils.GenomicsFactory;

import java.io.IOException;
import java.io.Serializable;
import java.security.GeneralSecurityException;

/**
 * Helper class to perform authentication when making API calls
 */
public class GenomicsAuth implements Serializable {
  private final String applicationName;
  private final String accessToken;
  private final String apiKey;
  
  private GenomicsAuth(String applicationName, String accessToken, String apiKey) {
    this.applicationName = applicationName;
    this.accessToken = accessToken;
    this.apiKey = apiKey;
  }
  
  public static GenomicsAuth fromAccessToken(String applicationName, String accessToken) {
    return new GenomicsAuth(applicationName, accessToken, null);
  }
  
  public static GenomicsAuth fromApiKey(String applicationName, String apiKey) {
    return new GenomicsAuth(applicationName, null, apiKey);
  }
  
  public Genomics getService() throws IOException, GeneralSecurityException {
    GenomicsFactory factory = GenomicsFactory.builder(applicationName).build();
    return (apiKey != null) ? factory.fromApiKey(apiKey) :
        factory.fromCredential(new GoogleCredential().setAccessToken(accessToken));
  }
}
