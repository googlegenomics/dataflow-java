/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dataflow.utils;

import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Contains common genomics pipeline options. Extend this class to add additional command line args.
 *
 *  Note: All methods defined in this class will be called during command line parsing unless it is
 * annotated with a @JsonIgnore annotation.
 */
public interface GenomicsOptions extends PipelineOptions {

  public static class Methods {

    /**
     * Gets access token for this pipeline.
     */
    private static String getAccessToken(GenomicsOptions options) throws IOException,
        GeneralSecurityException {
      return GenomicsFactory.builder(options.getAppName()).build()
          .makeCredential(new File(options.getSecretsFile())).getAccessToken();
    }

    /**
     * Gets a GenomicsAuth object using credentials from this options
     *
     * @return GenomicsAuth object used to authenticate API calls
     *
     * @throws IOException
     * @throws GeneralSecurityException
     */
    public static GenomicsAuth getGenomicsAuth(GenomicsOptions options)
        throws IOException, GeneralSecurityException {
      String apiKey = options.getApiKey(), appName = options.getAppName();
      return (apiKey != null)
          ? GenomicsAuth.fromApiKey(appName, apiKey)
          : GenomicsAuth.fromAccessToken(appName, getAccessToken(options));
    }

    /**
     * Makes sure options are valid.
     *
     * This method is automatically called during options parsing by the parser.
     */
    public static void validateOptions(GenomicsOptions options) {
      String secretsFile = options.getSecretsFile(), apiKey = options.getApiKey();
      if (secretsFile != null && apiKey != null) {
        throw new IllegalArgumentException(
            "Cannot use both --secretsFile and --apiKey");
      } else if (secretsFile == null && apiKey == null) {
        throw new IllegalArgumentException(
            "Need to specify either --secretsFile or --apiKey");
      }
    }
  }

  @Description("If querying a public dataset, provide a Google API key that has access "
      + "to genomics data and no OAuth will be performed.")
  String getApiKey();

  void setApiKey(String apiKey);
}