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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.common.base.Preconditions;

import java.util.Iterator;

public abstract class AbstractPInput<I extends AbstractPInput<I>> implements PInput {

  @SuppressWarnings("unchecked")
  public final <O extends POutput> O apply(PTransform<? super I, O> transform) {
    return Pipeline.applyTransform((I) this, transform);
  }

  @Override public final Pipeline getPipeline() {
    Iterator<? extends PValue> iterator = expand().iterator();
    Preconditions.checkState(iterator.hasNext());
    Pipeline pipeline = iterator.next().getPipeline();
    while (iterator.hasNext()) {
      Preconditions.checkState(pipeline == iterator.next().getPipeline());
    }
    return pipeline;
  }

  @Override public final void finishSpecifying() {
    for (PValue pValue : expand()) {
      pValue.finishSpecifying();
    }
  }
}