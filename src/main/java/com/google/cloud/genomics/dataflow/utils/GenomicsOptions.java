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

import com.google.cloud.dataflow.sdk.runners.Description;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Contains common genomics pipeline options.
 * Extend this class to add additional command line args.
 * 
 * Note: All methods defined in this class will be called during command line parsing unless
 * it is annotated with a @JsonIgnore annotation.
 */
public class GenomicsOptions extends PipelineOptions {
  @Description("If querying a public dataset, provide a Google API key that has access " +
      "to genomics data and no OAuth will be performed.")
  public String apiKey;

  @Description("If querying a private dataset, or performing any write operations, " +
      "you need to provide the path to client_secrets.json. Do not supply an api key.")
  public String clientSecretsFilename;
  
  @Description("Name of the application for oauth purposes. Defaults to GoogleGenomicsApp")
  public String applicationName = "GoogleGenomicsApp";
  
  /**
   * Makes sure options are valid.
   * 
   * This method is automatically called during options parsing by the parser.
   */
  public void validateOptions() {
    if (clientSecretsFilename != null && apiKey != null) {
      throw new IllegalArgumentException("Cannot use both a client secrets file and api key!");
    } else if (clientSecretsFilename == null && apiKey == null) {
      throw new IllegalArgumentException("Need to specify either clientSecretsFilename or apiKey!");
    }
  }
  
  /**
   * Gets access token for this pipeline.
   */
  @JsonIgnore
  private String getAccessToken() throws IOException, GeneralSecurityException {
    return GenomicsFactory.builder(applicationName).build()
        .makeCredential(new File(clientSecretsFilename)).getAccessToken();
  }
  
  /**
   * Gets a GenomicsAuth object using credentials from this options
   * 
   * @return GenomicsAuth object used to authenticate API calls
   * 
   * @throws IOException
   * @throws GeneralSecurityException
   */
  @JsonIgnore
  public GenomicsAuth getGenomicsAuth() 
      throws IOException, GeneralSecurityException {
    return (apiKey != null) ? 
        GenomicsAuth.fromApiKey(applicationName, apiKey) :
        GenomicsAuth.fromAccessToken(applicationName, getAccessToken());
  }
}
