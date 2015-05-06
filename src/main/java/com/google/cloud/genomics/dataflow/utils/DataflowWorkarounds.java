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

import static com.google.common.collect.Lists.newArrayList;

import java.net.URL;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import com.google.cloud.dataflow.sdk.coders.CoderFactory;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import com.google.api.client.json.GenericJson;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

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
        new CoderFactory() {

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
}
