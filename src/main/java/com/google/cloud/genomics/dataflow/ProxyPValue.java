package com.google.cloud.genomics.dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;

import java.util.Collection;

public abstract class ProxyPValue<D extends PValue, P extends ProxyPValue<D, P>> extends PValue {

  private final D delegate;

  protected ProxyPValue(D delegate) {
    this.delegate = delegate;
  }

  @SuppressWarnings("unchecked")
  public final <Output extends POutput> Output apply(PTransform<? super P, Output> transform) {
    return Pipeline.applyTransform((P) this, transform);
  }

  protected final D delegate() {
    return delegate;
  }

  @Override public final Collection<? extends PValue> expand() {
    return delegate.expand();
  }

  @Override public final void finishSpecifying() {
    delegate.finishSpecifying();
  }

  @Override public final void finishSpecifyingOutput() {
    delegate.finishSpecifyingOutput();
  }

  @Override public final String getName() {
    return delegate.getName();
  }

  @Override public final Pipeline getPipeline() {
    return delegate.getPipeline();
  }

  @Override public final PTransform<?, ?> getProducingTransformInternal() {
    return delegate.getProducingTransformInternal();
  }

  @Override public final boolean isFinishedSpecifyingInternal() {
    return delegate.isFinishedSpecifyingInternal();
  }

  @Override public final void recordAsOutput(Pipeline pipeline, PTransform<?, ?> transform) {
    delegate.recordAsOutput(pipeline, transform);
  }

  @Override public final void recordAsOutput(
      Pipeline pipeline, PTransform<?, ?> transform, String outName) {
    delegate.recordAsOutput(pipeline, transform, outName);
  }

  @Override public final PValue setName(String name) {
    return delegate.setName(name);
  }

  @Override public final PValue setPipelineInternal(Pipeline pipeline) {
    return delegate.setPipelineInternal(pipeline);
  }
}