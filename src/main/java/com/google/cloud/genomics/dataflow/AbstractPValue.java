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

import java.util.Collection;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PObjectTuple;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.common.base.Optional;

/**
 * An abstract supertype for user-defined PValues
 */
public abstract class AbstractPValue<V, T extends AbstractPValue<V, T>> extends PValue {

  private final PCollection<V> collection;
  private final Optional<PObjectTuple> sideInputs;

  protected AbstractPValue(PCollection<V> collection, Optional<PObjectTuple> sideInputs) {
    this.collection = collection;
    this.sideInputs = sideInputs;
  }

  @SuppressWarnings("unchecked")
  public final <Output extends POutput> Output apply(PTransform<? super T, Output> transform) {
    return Pipeline.applyTransform((T) this, transform);
  }

  public final PCollection<V> collection() {
    return collection;
  }

  @Override public final Collection<? extends PValue> expand() {
    return collection().expand();
  }

  @Override public final void finishSpecifying() {
    collection().finishSpecifying();
  }

  @Override public final void finishSpecifyingOutput() {
    collection().finishSpecifyingOutput();
  }

  @Override public final Pipeline getPipeline() {
    return collection().getPipeline();
  }

  @Override public final PTransform<?, ?> getProducingTransformInternal() {
    return collection().getProducingTransformInternal();
  }

  @Override public final boolean isFinishedSpecifyingInternal() {
    return collection().isFinishedSpecifyingInternal();
  }

  @Override public final void recordAsOutput(Pipeline pipeline, PTransform<?, ?> transform) {
    collection().recordAsOutput(pipeline, transform);
  }

  @Override public final void recordAsOutput(
      Pipeline pipeline, PTransform<?, ?> transform, String outName) {
    collection().recordAsOutput(pipeline, transform, outName);
  }

  @Override public final PValue setPipelineInternal(Pipeline pipeline) {
    return collection().setPipelineInternal(pipeline);
  }

  public final Optional<PObjectTuple> sideInputs() {
    return sideInputs;
  }
}