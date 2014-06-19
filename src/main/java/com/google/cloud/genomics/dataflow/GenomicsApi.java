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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.extensions.java6.auth.oauth2.GooglePromptReceiver;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.GenomicsRequest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.List;

public class GenomicsApi {
  public static final String GENOMICS_SCOPE = "https://www.googleapis.com/auth/genomics";
  private static final int API_RETRIES = 3;

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final java.io.File DATA_STORE_DIR =
      new java.io.File(System.getProperty("user.home"), ".store/genomics_dataflow_client");

  private final String accessToken;
  private final String apiKey;
  private Genomics service;

  public GenomicsApi(String accessToken, String apiKey) {
    this.accessToken = accessToken;
    this.apiKey = apiKey;
  }

  /**
   * Use this function to get a valid access token from the user before running a dataflow pipeline.
   *
   * @param clientSecretsFilename Where the client_secrets.json file is located.
   *     Usually this is an option specified by the user from the command line.
   * @param scopes The OAuth scopes needed by the pipeline.
   *     Common scopes are constants in this class.
   * @return An access token that can be used to make a GoogleCredential:
   *     new GoogleCredential().setAccessToken(accessToken)
   */
  public static String getAccessToken(String clientSecretsFilename, List<String> scopes)
      throws GeneralSecurityException, IOException {
    GoogleClientSecrets clientSecrets = loadClientSecrets(clientSecretsFilename);
    FileDataStoreFactory dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);

    NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
        httpTransport, JSON_FACTORY, clientSecrets, scopes).setAccessType("offline")
        .setDataStoreFactory(dataStoreFactory).build();
    Credential credential = new AuthorizationCodeInstalledApp(flow, new GooglePromptReceiver()).authorize("dataflow");
    return credential.getAccessToken();
  }

  private static GoogleClientSecrets loadClientSecrets(String clientSecretsFilename)
      throws IOException {
    File f = new File(clientSecretsFilename);
    if (f.exists()) {
      InputStream inputStream = new FileInputStream(new File(clientSecretsFilename));
      return GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(inputStream));
    }

    throw new RuntimeException("Please provide an --apiKey option or a valid client_secrets.json " +
        "file. Client secrets file " + clientSecretsFilename + " does not exist. " +
        "Visit https://developers.google.com/genomics to learn how to get an api key or install " +
        "a client_secrets.json file. If you have installed a client_secrets.json in a specific " +
        "location, use --clientSecretsFilename <path>/client_secrets.json.");
  }

  public Genomics getService() {
    if (service == null) {
      GoogleCredential credential = accessToken == null ? null :
          new GoogleCredential().setAccessToken(accessToken);
      try {
        service = new Genomics.Builder(GoogleNetHttpTransport.newTrustedTransport(),
            new JacksonFactory(), credential).setApplicationName("dataflow-reader").build();
      } catch (GeneralSecurityException | IOException e) {
        throw new RuntimeException("Unable to create the Genomics service", e);
      }
    }
    return service;
  }

  public <T> T executeRequest(GenomicsRequest<T> search, String fields) {
    for (int i = 0; i < API_RETRIES; i++) {
      try {
        return search.setFields(fields).setKey(apiKey).execute();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    throw new RuntimeException("Genomics API call failed multiple times in a row.");
  }
}
