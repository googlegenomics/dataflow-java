package com.google.cloud.genomics.dataflow.utils;

import static org.junit.Assert.*;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

public class PairGeneratorTest {

  @Test
  public void testAllPairsWithReplacement() {
    FluentIterable<KV<String, String>> pairs =
        PairGenerator.<String, ImmutableList<String>>allPairs(
            ImmutableList.of("one", "two", "three"), true);
    assertEquals(6, pairs.size());
    Assert.assertThat(
        pairs,
        CoreMatchers.hasItems(KV.of("one", "one"), KV.of("one", "two"), KV.of("one", "three"),
            KV.of("two", "two"), KV.of("two", "three"), KV.of("three", "three")));
  }

  @Test
  public void testAllPairsWithoutReplacement() {
    FluentIterable<KV<String, String>> pairs =
        PairGenerator.<String, ImmutableList<String>>allPairs(
            ImmutableList.of("one", "two", "three"), false);
    assertEquals(3, pairs.size());
    Assert.assertThat(pairs,
        CoreMatchers.hasItems(KV.of("one", "two"), KV.of("one", "three"), KV.of("two", "three")));
  }
}
