/*
 * Copyright (C) 2016 Google Inc.
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
package com.google.cloud.genomics.dataflow.functions;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.Keys;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/*
 * Breaks Dataflow fusion by doing GroupByKey/Ungroup that forces materialization of the data,
 * thus preventing Dataflow form fusing steps before and after this transform.
 * This is useful to insert in cases where a series of transforms deal with very small sets of data
 * that act as descriptors of very heavy workloads in subsequent steps (e.g. a collection of file names
 * where each file takes a long time to process).
 * In this case Dataflow might over-eagerly fuse steps dealing with small datasets with the "heavy" 
 * processing steps, which will result in heavy steps being executed on a single worker.
 * If you insert a fusion break transform in between then Dataflow will be able to spin up many 
 * parallel workers to handle the heavy processing.
 * @see https://cloud.google.com/dataflow/service/dataflow-service-desc#Optimization
 * Typical usage:
 *  ...
 *  PCollection<String> fileNames = pipeline.apply(...);
 *  fileNames.apply(new BreakFusionTransform<String>())
 *      .apply(new HeavyFileProcessingTransform())
 *      .....
 */
public class BreakFusionTransform<T> extends PTransform<PCollection<T>, PCollection<T>> {
  
  public BreakFusionTransform() {
    super("Break Fusion Transform");

  }
  
  @Override
  public PCollection<T> apply(PCollection<T> input) {
    return input
        .apply(
            ParDo.named("Break fusion mapper")
              .of(new DummyMapFn<T>()))  
        .apply(GroupByKey.<T, Integer>create())
        .apply(Keys.<T>create());  
  }
  
  
   static class DummyMapFn<T> extends DoFn<T, KV<T, Integer>> {  
    private static final int DUMMY_VALUE = 42;

    @Override
    public void processElement(DoFn<T, KV<T, Integer>>.ProcessContext c) throws Exception {
      c.output( KV.of(c.element(), DUMMY_VALUE));
    }
   } 
}

