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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.logging.Logger;

/**
 * Contains dataflow-related workarounds.
 */
public class DataflowWorkarounds {
  private static final Logger LOG = Logger.getLogger(DataflowWorkarounds.class.getName());

  /**
   * Change a flat list of sharding options into a flattened PCollection to force dataflow to use
   * multiple workers. In the future, this shouldn't be necessary.
   */
  public static <T> PCollection<T> getPCollection(List<T> shardOptions, Coder<T> coder,
      Pipeline p, double numWorkers) {

    LOG.info("Turning " + shardOptions.size() + " options into " + numWorkers + " workers");
    numWorkers = Math.min(shardOptions.size(), numWorkers);

    int optionsPerWorker = (int) Math.ceil(shardOptions.size() / numWorkers);
    List<PCollection<T>> pCollections = Lists.newArrayList();

    for (int i = 0; i < numWorkers; i++) {
      int start = i * optionsPerWorker;
      int end = Math.min(shardOptions.size(), start + optionsPerWorker);

      LOG.info("Adding collection with " + start + " to " + end);
      pCollections.add(p.begin().apply(Create.of(shardOptions.subList(start, end))).setCoder(coder));
    }

    return PCollectionList.of(pCollections).apply(new Flatten<T>());
  }
}
