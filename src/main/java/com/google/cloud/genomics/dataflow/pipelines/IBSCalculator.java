/*
Copyright 2014 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.google.cloud.genomics.dataflow.pipelines;

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.common.math.DoubleMath;

final class IBSCalculator implements
    SerializableFunction<Iterable<Double>, Double> {
  @Override
  public Double apply(Iterable<Double> ratiosOfSharedAlleles) {
    return DoubleMath.mean(ratiosOfSharedAlleles);
  }
}