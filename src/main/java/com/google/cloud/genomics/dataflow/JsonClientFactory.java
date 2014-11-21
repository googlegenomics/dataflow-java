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
package com.google.cloud.genomics.dataflow;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.java6.auth.oauth2.VerificationCodeReceiver;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.Key;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.CreatePObject;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.utils.JsonClientBuilder;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collection;

public final class JsonClientFactory extends GenericJson {

  public static PObject<JsonClientFactory> of(
      HttpTransport transport,
      JsonFactory jsonFactory,
      Pipeline pipeline,
      Collection<String> scopes) throws IOException {
    return of(
        transport,
        jsonFactory,
        pipeline,
        new LocalServerReceiver(),
        scopes);
  }

  public static PObject<JsonClientFactory> of(
      HttpTransport transport,
      JsonFactory jsonFactory,
      Pipeline pipeline,
      String... scopes) throws IOException {
    return of(
        transport,
        jsonFactory,
        pipeline,
        new LocalServerReceiver(),
        scopes);
  }

  public static PObject<JsonClientFactory> of(
      HttpTransport transport,
      JsonFactory jsonFactory,
      Pipeline pipeline,
      VerificationCodeReceiver verificationCodeReceiver,
      Collection<String> scopes) throws IOException {
    PipelineOptions options = pipeline.getOptions();
    try (Reader in = new FileReader(options.getSecretsFile())) {
      String applicationName = options.getAppName();
      GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(jsonFactory, in);
      AuthorizationCodeInstalledApp installedApp = new AuthorizationCodeInstalledApp(
          new GoogleAuthorizationCodeFlow
              .Builder(transport, jsonFactory, clientSecrets, scopes)
              .setDataStoreFactory(new FileDataStoreFactory(new File(
                  System.getProperty("user.home"),
                  String.format(".store/%s", applicationName.replace("/", "_")))))
              .build(),
          verificationCodeReceiver);
      Credential
          credential = installedApp.authorize(System.getProperty("user.name"));
      return pipeline
          .apply(CreatePObject.of(new JsonClientFactory()
              .setApplicationName(applicationName)
              .setClientSecrets(clientSecrets)
              .setTokenResponse(new TokenResponse()
                  .setAccessToken(credential.getAccessToken())
                  .setExpiresInSeconds(credential.getExpiresInSeconds())
                  .setRefreshToken(credential.getRefreshToken()))))
          .setCoder(GenericJsonCoder.of(JsonClientFactory.class));
    }
  }

  public static PObject<JsonClientFactory> of(
      HttpTransport transport,
      JsonFactory jsonFactory,
      Pipeline pipeline,
      VerificationCodeReceiver verificationCodeReceiver,
      String... scopes) throws IOException {
    return of(
        transport,
        jsonFactory,
        pipeline,
        verificationCodeReceiver,
        Arrays.asList(scopes));
  }

  public static PObject<JsonClientFactory> of(
      Pipeline pipeline,
      Collection<String> scopes) throws IOException {
    return of(
        Utils.getDefaultTransport(),
        Utils.getDefaultJsonFactory(),
        pipeline,
        scopes);
  }

  public static PObject<JsonClientFactory> of(
      Pipeline pipeline,
      String... scopes) throws IOException {
    return of(
        Utils.getDefaultTransport(),
        Utils.getDefaultJsonFactory(),
        pipeline,
        scopes);
  }

  public static PObject<JsonClientFactory> of(
      Pipeline pipeline,
      VerificationCodeReceiver verificationCodeReceiver,
      Collection<String> scopes) throws IOException {
    return of(
        Utils.getDefaultTransport(),
        Utils.getDefaultJsonFactory(),
        pipeline,
        verificationCodeReceiver,
        scopes);
  }

  public static PObject<JsonClientFactory> of(
      Pipeline pipeline,
      VerificationCodeReceiver verificationCodeReceiver,
      String... scopes) throws IOException {
    return of(
        Utils.getDefaultTransport(),
        Utils.getDefaultJsonFactory(),
        pipeline,
        verificationCodeReceiver,
        scopes);
  }

  @Key("application_name") private String applicationName;
  @Key("client_secrets") private GoogleClientSecrets clientSecrets;
  @Key("token_response") private TokenResponse tokenResponse;

  public <C extends AbstractGoogleJsonClient, B extends AbstractGoogleJsonClient.Builder>
      C createClient(
          HttpTransport transport,
          JsonFactory jsonFactory,
          JsonClientBuilder.Implementation<? extends C, B> implementation) {
    return JsonClientBuilder.create(applicationName)
        .setTransport(transport)
        .setJsonFactory(jsonFactory)
        .setCredential(new GoogleCredential.Builder()
            .setTransport(transport)
            .setJsonFactory(jsonFactory)
            .setClientSecrets(clientSecrets)
            .build()
            .setFromTokenResponse(tokenResponse))
        .build(implementation);
  }

  public <C extends AbstractGoogleJsonClient, B extends AbstractGoogleJsonClient.Builder>
      C createClient(JsonClientBuilder.Implementation<? extends C, B> implementation) {
    return createClient(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory(), implementation);
  }

  public String getApplicationName() {
    return applicationName;
  }

  public GoogleClientSecrets getClientSecrets() {
    return clientSecrets;
  }

  public TokenResponse getTokenResponse() {
    return tokenResponse;
  }

  public JsonClientFactory setApplicationName(String applicationName) {
    this.applicationName = applicationName;
    return this;
  }

  public JsonClientFactory setClientSecrets(GoogleClientSecrets clientSecrets) {
    this.clientSecrets = clientSecrets;
    return this;
  }

  public JsonClientFactory setTokenResponse(TokenResponse tokenResponse) {
    this.tokenResponse = tokenResponse;
    return this;
  }
}