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
package com.google.cloud.genomics.dataflow.coders;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public abstract class DelegatingStandardCoder<X, Y> extends StandardCoder<X> {

  private final Coder<Y> delegate;

  protected DelegatingStandardCoder(Coder<Y> delegate) {
    this.delegate = delegate;
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return delegate.getCoderArguments();
  }

  @Override public final X decode(InputStream inStream, Context context)
      throws CoderException, IOException {
    return from(delegate.decode(inStream, context));
  }

  @Override public final void encode(X value, OutputStream outStream, Context context)
      throws CoderException, IOException {
    delegate.encode(to(value), outStream, context);
  }

  protected abstract X from(Y object) throws CoderException, IOException;

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    delegate.verifyDeterministic();
  }

  protected abstract Y to(X object) throws CoderException, IOException;
}