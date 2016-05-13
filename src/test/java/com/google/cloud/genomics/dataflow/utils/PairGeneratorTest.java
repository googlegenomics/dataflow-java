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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PairGeneratorTest {

  @Test
  public void testAllPairsWithReplacement() {
    FluentIterable<KV<String, String>> pairs =
        PairGenerator.WITH_REPLACEMENT.allPairs(ImmutableList.of("one", "two", "three"),
            String.CASE_INSENSITIVE_ORDER);
    assertEquals(6, pairs.size());
    assertThat(
        pairs,
        CoreMatchers.hasItems(KV.of("one", "one"), KV.of("one", "two"), KV.of("one", "three"),
            KV.of("two", "two"), KV.of("three", "two"), KV.of("three", "three")));
  }

  @Test
  public void testAllPairsWithoutReplacement() {
    FluentIterable<KV<String, String>> pairs =
        PairGenerator.WITHOUT_REPLACEMENT.allPairs(ImmutableList.of("one", "two", "three"),
            String.CASE_INSENSITIVE_ORDER);
    assertEquals(3, pairs.size());
    assertThat(pairs,
        CoreMatchers.hasItems(KV.of("one", "two"), KV.of("one", "three"), KV.of("three", "two")));
  }
}
