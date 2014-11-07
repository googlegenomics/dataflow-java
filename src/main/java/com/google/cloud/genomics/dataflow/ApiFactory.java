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
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.extensions.java6.auth.oauth2.GooglePromptReceiver;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Key;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.CreatePObject;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

public final class ApiFactory extends GenericJson {

  public interface Implementation<C extends AbstractGoogleJsonClient> extends Serializable {
    C createClient(
        HttpTransport transport,
        JsonFactory jsonFactory,
        HttpRequestInitializer httpRequestInitializer,
        String appName);
  }

  public static PObject<ApiFactory> of(Pipeline pipeline, Collection<String> scopes)
      throws IOException {
    PipelineOptions options = pipeline.getOptions();
    try (Reader in = new FileReader(options.secretsFile)) {
      JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
      GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(jsonFactory, in);
      GoogleAuthorizationCodeFlow codeFlow = new GoogleAuthorizationCodeFlow
          .Builder(Utils.getDefaultTransport(), jsonFactory, clientSecrets, scopes)
          .setDataStoreFactory(
              new FileDataStoreFactory(new File(options.getCredentialDirOrDefault())))
          .build();
      Credential credential =
          new AuthorizationCodeInstalledApp(codeFlow, new GooglePromptReceiver())
              .authorize(options.credentialId);
      return pipeline
          .apply(CreatePObject.of(new ApiFactory()
              .setAppName(options.appName)
              .setClientSecrets(clientSecrets)
              .setTokenResponse(new TokenResponse()
                  .setAccessToken(credential.getAccessToken())
                  .setRefreshToken(credential.getRefreshToken())
                  .setExpiresInSeconds(credential.getExpiresInSeconds()))))
          .setCoder(GenericJsonCoder.of(ApiFactory.class));
    }
  }

  public static PObject<ApiFactory> of(Pipeline pipeline, String... scopes)
      throws IOException {
    return of(pipeline, Arrays.asList(scopes));
  }

  @Key("app_name") private String appName;
  @Key("client_secrets") private GoogleClientSecrets clientSecrets;
  @Key("token_response") private TokenResponse tokenResponse;

  @Override public ApiFactory clone() {
    return (ApiFactory) super.clone();
  }

  public <C extends AbstractGoogleJsonClient> C createApi(
      Implementation<? extends C> implementation) {
    final HttpTransport transport = Utils.getDefaultTransport();
    final JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
    return implementation.createClient(
        transport,
        jsonFactory,
        new HttpRequestInitializer() {

          private final GoogleCredential credential = new GoogleCredential.Builder()
              .setTransport(transport)
              .setJsonFactory(jsonFactory)
              .setClientSecrets(getClientSecrets())
              .build()
              .setFromTokenResponse(getTokenResponse());

          private final HttpUnsuccessfulResponseHandler unsuccessfulResponseHandler =
              new HttpBackOffUnsuccessfulResponseHandler(new ExponentialBackOff());

          @Override public void initialize(HttpRequest request) throws IOException {
            credential.initialize(request);
            request.setUnsuccessfulResponseHandler(unsuccessfulResponseHandler);
          }
        },
        appName);
  }

  public String getAppName() {
    return appName;
  }

  public GoogleClientSecrets getClientSecrets() {
    return clientSecrets;
  }

  public TokenResponse getTokenResponse() {
    return tokenResponse;
  }

  @Override public ApiFactory set(String fieldName, Object value) {
    return (ApiFactory) super.set(fieldName, value);
  }

  public ApiFactory setAppName(String appName) {
    this.appName = appName;
    return this;
  }

  public ApiFactory setClientSecrets(GoogleClientSecrets clientSecrets) {
    this.clientSecrets = clientSecrets;
    return this;
  }

  public ApiFactory setTokenResponse(TokenResponse tokenResponse) {
    this.tokenResponse = tokenResponse;
    return this;
  }
}