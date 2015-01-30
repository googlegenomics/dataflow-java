package com.google.cloud.genomics.dataflow.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

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
