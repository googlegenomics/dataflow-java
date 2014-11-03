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
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.Key;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.CreatePObject;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.Credentials;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.common.base.Throwables;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.security.GeneralSecurityException;

public final class ApiFactory extends GenericJson {

  public interface Implementation<
      C extends AbstractGoogleJsonClient,
      B extends AbstractGoogleJsonClient.Builder> {

    C build(B builder);

    B newBuilder(HttpTransport transport, JsonFactory factory, HttpRequestInitializer initializer);
  }

  public static PTransform<PInput, PObject<ApiFactory>> of() {
    return new PTransform<PInput, PObject<ApiFactory>>() {
          @Override public PObject<ApiFactory> apply(PInput input) {
            Pipeline pipeline = input.getPipeline();
            PipelineOptions options = pipeline.getOptions();
            try (Reader in = new FileReader(options.secretsFile)) {
              Credential credential = Credentials.getUserCredential(options);
              return pipeline
                  .apply(CreatePObject.of(new ApiFactory()
                      .setClientSecrets(GoogleClientSecrets.load(Utils.getDefaultJsonFactory(), in))
                      .setTokenResponse(new TokenResponse()
                          .setAccessToken(credential.getAccessToken())
                          .setRefreshToken(credential.getRefreshToken())
                          .setExpiresInSeconds(credential.getExpiresInSeconds()))))
                  .setCoder(GenericJsonCoder.of(ApiFactory.class));
            } catch (GeneralSecurityException | IOException e) {
              throw Throwables.propagate(e);
            }
          }
        };
  }

  @Key("client_secrets") private GoogleClientSecrets clientSecrets;
  @Key("token_response") private TokenResponse tokenResponse;

  @Override public ApiFactory clone() {
    return (ApiFactory) super.clone();
  }

  public <C extends AbstractGoogleJsonClient,
      B extends AbstractGoogleJsonClient.Builder> C createApi(
      Implementation<? extends C, B> implementation) {
    HttpTransport transport = Utils.getDefaultTransport();
    JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
    return implementation.build(implementation.newBuilder(
        transport,
        jsonFactory,
        new GoogleCredential.Builder()
            .setTransport(transport)
            .setJsonFactory(jsonFactory)
            .setClientSecrets(getClientSecrets())
            .build()
            .setFromTokenResponse(getTokenResponse())));
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
