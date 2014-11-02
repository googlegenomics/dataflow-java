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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Collection;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.java6.auth.oauth2.VerificationCodeReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.extensions.java6.auth.oauth2.GooglePromptReceiver;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.api.client.util.Key;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.CreatePObject;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.common.base.Preconditions;

public final class ApiFactory extends GenericJson {

  public static final class Builder {

    private static <X> X checkNotNull(X reference, String method, String type) {
      return Preconditions.checkNotNull(
          reference, String.format("Must call ApiFactory.Builder#%s(%s)", method, type));
    }

    private GoogleClientSecrets clientSecrets;
    private DataStoreFactory dataStoreFactory;
    private JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
    private VerificationCodeReceiver receiver = new GooglePromptReceiver();
    private Collection<String> scopes;
    private HttpTransport transport = Utils.getDefaultTransport();
    private String userId;

    private Builder() {}

    public PTransform<PInput, PObject<ApiFactory>> build() {
      final GoogleClientSecrets clientSecrets = checkNotNull(
          this.clientSecrets, "setClientSecrets", "GoogleClientSecrets");
      final DataStoreFactory dataStoreFactory = checkNotNull(
          this.dataStoreFactory, "setDataStoreFactory", "DataStoreFactory");
      final Collection<String> scopes = checkNotNull(
          this.scopes, "setScopes", "Collection<String>");
      final String userId = checkNotNull(
          this.userId, "setUserId", "String");
      return new PTransform<PInput, PObject<ApiFactory>>() {
            @Override public PObject<ApiFactory> apply(PInput input) {
              try {
                Pipeline pipeline = input.getPipeline();
                GoogleAuthorizationCodeFlow codeFlow = new GoogleAuthorizationCodeFlow
                    .Builder(transport, jsonFactory, clientSecrets, scopes)
                    .setDataStoreFactory(dataStoreFactory)
                    .build();
                Credential credential = new AuthorizationCodeInstalledApp(codeFlow, receiver)
                    .authorize(userId);
                return pipeline
                    .apply(CreatePObject.of(new ApiFactory()
                        .setTokenResponse(new TokenResponse()
                            .setAccessToken(credential.getAccessToken())
                            .setRefreshToken(credential.getRefreshToken())
                            .setExpiresInSeconds(credential.getExpiresInSeconds()))
                        .setClientSecrets(clientSecrets)))
                    .setCoder(GenericJsonCoder.of(ApiFactory.class));
              } catch (IOException e) {
                throw Throwables.propagate(e);
              }
            }
          };
    }

    public Builder setClientSecrets(GoogleClientSecrets clientSecrets) {
      this.clientSecrets =
          Preconditions.checkNotNull(clientSecrets, "clientSecrets was null");
      return this;
    }

    public Builder setDataStoreFactory(DataStoreFactory dataStoreFactory) {
      this.dataStoreFactory =
          Preconditions.checkNotNull(dataStoreFactory, "dataStoreFactory was null");
      return this;
    }

    public Builder setJsonFactory(JsonFactory jsonFactory) {
      this.jsonFactory =
          Preconditions.checkNotNull(jsonFactory, "jsonFactory was null");
      return this;
    }

    public Builder setReceiver(VerificationCodeReceiver receiver) {
      this.receiver =
          Preconditions.checkNotNull(receiver, "receiver was null");
      return this;
    }

    public Builder setScopes(Collection<String> scopes) {
      Preconditions.checkArgument(
          !(this.scopes = Preconditions.checkNotNull(scopes, "scopes was null")).isEmpty(),
          "scopes was empty");
      return this;
    }

    public Builder setTransport(HttpTransport transport) {
      this.transport =
          Preconditions.checkNotNull(transport, "transport was null");
      return this;
    }

    public Builder setUserId(String userId) {
      this.userId =
          Preconditions.checkNotNull(userId, "userId was null");
      return this;
    }
  }

  public interface Implementation<
      C extends AbstractGoogleJsonClient,
      B extends AbstractGoogleJsonClient.Builder> {

    C build(B builder);

    B newBuilder(HttpTransport transport, JsonFactory factory, HttpRequestInitializer initializer);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder fromOptions(JsonFactory jsonFactory, PipelineOptions options)
      throws IOException {
    try (Reader in = new FileReader(options.secretsFile)) {
      return builder()
          .setClientSecrets(GoogleClientSecrets.load(jsonFactory, in))
          .setDataStoreFactory(
              new FileDataStoreFactory(new File(options.getCredentialDirOrDefault())))
          .setJsonFactory(jsonFactory)
          .setUserId(options.credentialId);
    }
  }

  public static Builder fromOptions(PipelineOptions options) throws IOException {
    return fromOptions(Utils.getDefaultJsonFactory(), options);
  }

  @Key("client_secrets") private GoogleClientSecrets clientSecrets;
  @Key("token_response") private TokenResponse tokenResponse;

  @Override public ApiFactory clone() {
    return (ApiFactory) super.clone();
  }

  public <C extends AbstractGoogleJsonClient, B extends AbstractGoogleJsonClient.Builder> C
      createApi(HttpTransport transport, JsonFactory jsonFactory, Implementation<? extends C, B>
      implementation) {
    return implementation.build(implementation.newBuilder(
        transport,
        jsonFactory,
        new GoogleCredential.Builder()
            .setTransport(transport)
            .setJsonFactory(jsonFactory)
            .setClientSecrets(clientSecrets)
            .build()
            .setFromTokenResponse(tokenResponse)));
  }

  public <C extends AbstractGoogleJsonClient, B extends AbstractGoogleJsonClient.Builder>
      C createApi(Implementation<? extends C, B> implementation) {
    return createApi(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory(), implementation);
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

  public ApiFactory setClientSecrets(GoogleClientSecrets clientSecrets) {
    this.clientSecrets = clientSecrets;
    return this;
  }

  public ApiFactory setTokenResponse(TokenResponse tokenResponse) {
    this.tokenResponse = tokenResponse;
    return this;
  }
}