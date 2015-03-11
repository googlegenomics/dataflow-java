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
package com.google.cloud.genomics.dataflow.utils;

import com.google.api.client.json.GenericJson;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRegistrar.Options;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedInts;

import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.net.URL;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Contains dataflow-related workarounds.
 */
public class DataflowWorkarounds {
  private static final Logger LOG = Logger.getLogger(DataflowWorkarounds.class.getName());

  /**
   * Registers a coder for a given class so that we don't have to constantly call .setCoder
   */
  public static <T> void registerCoder(Pipeline p, Class<T> clazz, final Coder<T> coder) {
    CoderRegistry registry = p.getCoderRegistry();
    registry.registerCoder(clazz,
        new CoderRegistry.CoderFactory() {

          @Override public Coder<?> create(List<? extends Coder<?>> typeArgumentCoders) {
            return coder;
          }

          @Override public List<Object> getInstanceComponents(Object value) {
            return null;
          }
        });
    p.setCoderRegistry(registry);
  }
  
  /**
   * Shortcut for registering all genomics related classes in dataflow
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static void registerGenomicsCoders(Pipeline p) {
    LOG.info("Registering coders for genomics classes");
    
    List<ClassLoader> classLoadersList = new LinkedList<ClassLoader>();
    classLoadersList.add(ClasspathHelper.contextClassLoader());
    classLoadersList.add(ClasspathHelper.staticClassLoader());

    Collection<URL> urls =
        newArrayList(Iterables.filter(
            ClasspathHelper.forClassLoader(classLoadersList.toArray(new ClassLoader[0])),
            new Predicate<URL>() {

              @Override
              public boolean apply(URL url) {
                return !url.toString().endsWith("jnilib") && !url.toString().endsWith("zip");
              }

            }));

    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .setScanners(new SubTypesScanner(), new ResourcesScanner())
        .setUrls(urls)
        .filterInputsBy(new FilterBuilder().include(
            FilterBuilder.prefix("com.google.api.services.genomics.model"))));
    
    for (Class clazz : reflections.getSubTypesOf(GenericJson.class)) {
      LOG.info("Registering coder for " + clazz.getSimpleName());
      DataflowWorkarounds.registerCoder(p, clazz, GenericJsonCoder.of(clazz));
    }
  }
  
  /**
   * Change a flat list of sharding options into a flattened PCollection to force dataflow to use
   * multiple workers. In the future, this shouldn't be necessary.
   */
  public static <T> PCollection<T> getPCollection(List<T> shardOptions, Coder<T> coder,
      Pipeline p) {
    
    DataflowPipelineOptions options = (GenomicsOptions) p.getOptions();
    int numWorkers = options.getNumWorkers();
    
    String [] machineNameParts = Iterables.toArray(Splitter.on('-').split(options.getMachineType()), String.class);
    if(3 == machineNameParts.length) {
      try {
        int numCores = UnsignedInts.parseUnsignedInt(machineNameParts[2]);
        numWorkers *= numCores;
      }
      catch(Exception e) {
        LOG.warning("Assuming one core per worker: " + e);
      }
    }
    
    LOG.info("Turning " + shardOptions.size() + " options into " + numWorkers + " workers");
    numWorkers = Math.min(shardOptions.size(), numWorkers);

    int optionsPerWorker = (int) Math.ceil(shardOptions.size() / numWorkers);
    List<PCollection<T>> pCollections = Lists.newArrayList();

    
    for (int i = 0; i < numWorkers; i++) {
      int start = i * optionsPerWorker;
      int end = Math.min(shardOptions.size(), start + optionsPerWorker);
      
      // It's possible for start >= end in the last worker,
      // in which case we'll just skip the collection.
      if (start >= end) {
        break;
      }
      
      LOG.info("Adding collection with " + start + " to " + end);
      PCollection<T> collection = p.begin().apply(Create.of(shardOptions.subList(start, end)));
      if (coder != null) {
        collection.setCoder(coder);
      }
      pCollections.add(collection);
    }

    return PCollectionList.of(pCollections).apply(Flatten.<T>create());
  }
  
  public static <T> PCollection<T> getPCollection(
      List<T> shardOptions, Pipeline p) {
    return getPCollection(shardOptions, null, p);
  }
}
