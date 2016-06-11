/*
 * Copyright (C) 2015 Google Inc.
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

import static com.google.common.collect.Lists.newArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.utils.grpc.VariantUtils;
import com.google.common.collect.Lists;
import com.google.genomics.v1.Variant;
import com.google.genomics.v1.VariantCall;

import org.hamcrest.CoreMatchers;
import org.hamcrest.collection.IsIterableWithSize;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@RunWith(JUnit4.class)
public class JoinNonVariantSegmentsWithVariantsTest {

  private static final List<VariantCall> variantCalls = Lists.newArrayList(VariantCall.newBuilder()
      .setCallSetName("het-alt sample").addGenotype(1).addGenotype(0).build(), VariantCall
      .newBuilder().setCallSetName("hom-alt sample").addGenotype(1).addGenotype(1).build());

  private static final List<VariantCall> blockRecord1Calls = Lists.newArrayList(VariantCall
      .newBuilder().setCallSetName("hom sample").addGenotype(0).addGenotype(0).build(), VariantCall
      .newBuilder().setCallSetName("no call sample").addGenotype(-1).addGenotype(-1).build());

  private static final List<VariantCall> blockRecord2Calls = Lists.newArrayList(VariantCall
      .newBuilder().setCallSetName("hom no-call sample").addGenotype(-1).addGenotype(0).build());

  private static final Variant expectedSnp1 = Variant.newBuilder().setReferenceName("chr7")
      .setStart(200010).setEnd(200011).setReferenceBases("A").addAlternateBases("C")
      .addAllCalls(variantCalls).addAllCalls(blockRecord1Calls).build();

  private static final Variant expectedSnp2 = Variant.newBuilder().setReferenceName("chr7")
      .setStart(200019).setEnd(200020).setReferenceBases("T").addAlternateBases("G")
      .addAllCalls(variantCalls).addAllCalls(blockRecord1Calls).addAllCalls(blockRecord2Calls)
      .build();

  private static final Variant expectedInsert = Variant.newBuilder().setReferenceName("chr7")
      .setStart(200010).setEnd(200011).setReferenceBases("A").addAlternateBases("AC")
      .addAllCalls(variantCalls).build();

  private Variant snp1;
  private Variant snp2;
  private Variant insert;
  private Variant blockRecord1;
  private Variant blockRecord2;
  private Variant[] input;

  @Before
  public void setUp() {
    snp1 =
        Variant.newBuilder().setReferenceName("chr7").setStart(200010).setEnd(200011)
            .setReferenceBases("A").addAlternateBases("C").addAllCalls(variantCalls).build();

    snp2 =
        Variant.newBuilder().setReferenceName("chr7").setStart(200019).setEnd(200020)
            .setReferenceBases("T").addAlternateBases("G").addAllCalls(variantCalls).build();

    insert =
        Variant.newBuilder().setReferenceName("chr7").setStart(200010).setEnd(200011)
            .setReferenceBases("A").addAlternateBases("AC").addAllCalls(variantCalls).build();

    blockRecord1 =
        Variant.newBuilder().setReferenceName("chr7").setStart(199005).setEnd(202050)
            .setReferenceBases("A").addAllCalls(blockRecord1Calls).build();

    blockRecord2 =
        Variant.newBuilder().setReferenceName("chr7").setStart(200011).setEnd(200020)
            .setReferenceBases("A").addAllCalls(blockRecord2Calls).build();

    input = new Variant[] {snp1, snp2, insert, blockRecord1, blockRecord2};
  }

  @Test
  public void testVariantVariantComparator() {
    Comparator<Variant> comparator = VariantUtils.NON_VARIANT_SEGMENT_COMPARATOR;

    assertEquals(-1, comparator.compare(blockRecord1, snp1));
    assertEquals(1, comparator.compare(blockRecord2, snp1));
    assertEquals(-1, comparator.compare(snp1, snp2));

    // Two variants at the same location
    Variant snp1DifferentAlt =
        Variant.newBuilder(snp1)
        .clearAlternateBases()
        .addAlternateBases("G")
        .build();
    assertTrue(0 > comparator.compare(snp1, snp1DifferentAlt));

    // Block record and variant at the same location
    Variant blockRecordForSnp1 =
        Variant.newBuilder(snp1)
        .clearAlternateBases()
        .build();
    assertEquals(1, comparator.compare(snp1, blockRecordForSnp1));

    List<Variant> variants = newArrayList(input);
    variants.add(snp1DifferentAlt);
    variants.add(blockRecordForSnp1);

    // Check all permutations
    for (Variant v1 : variants) {
      for (Variant v2 : variants) {
        assertTrue(Integer.signum(comparator.compare(v1, v2)) == -Integer.signum(comparator
            .compare(v2, v1)));
      }
    }
  }

  @Test
  public void testIsOverlapping() {
    assertTrue(VariantUtils.isOverlapping(blockRecord1, snp1));
    assertTrue(VariantUtils.isOverlapping(blockRecord1, snp2));
    assertFalse(VariantUtils.isOverlapping(blockRecord2, snp1));
    assertTrue(VariantUtils.isOverlapping(blockRecord2, snp2));
  }

  @Test
  public void testCombineVariantsFn() {
    DoFnTester<KV<KV<String, Long>, Iterable<Variant>>, Variant> fn =
      DoFnTester.of(new JoinNonVariantSegmentsWithVariants.CombineVariantsFn());

    Assert.assertThat(fn.processBatch(KV.of(KV.of("chr7", 200000L), (Iterable<Variant>) Arrays.asList(input))),
        CoreMatchers.hasItems(expectedSnp1, expectedSnp2, expectedInsert));
  }

  @Test
  public void testBinVariantsFn() {
    DoFnTester<Variant, KV<KV<String, Long>, Variant>> binVariantsFn =
        DoFnTester.of(new JoinNonVariantSegmentsWithVariants.BinShuffleAndCombineTransform.BinVariantsFn());

    List<KV<KV<String, Long>, Variant>> binVariantsOutput = binVariantsFn.processBatch(input);
    assertThat(binVariantsOutput, CoreMatchers.hasItem(KV.of(KV.of("chr7", 200000L), snp1)));
    assertThat(binVariantsOutput, CoreMatchers.hasItem(KV.of(KV.of("chr7", 200000L), snp2)));
    assertThat(binVariantsOutput, CoreMatchers.hasItem(KV.of(KV.of("chr7", 200000L), insert)));
    assertThat(binVariantsOutput, CoreMatchers.hasItem(KV.of(KV.of("chr7", 199000L), blockRecord1)));
    assertThat(binVariantsOutput, CoreMatchers.hasItem(KV.of(KV.of("chr7", 200000L), blockRecord1)));
    assertThat(binVariantsOutput, CoreMatchers.hasItem(KV.of(KV.of("chr7", 201000L), blockRecord1)));
    assertThat(binVariantsOutput, CoreMatchers.hasItem(KV.of(KV.of("chr7", 202000L), blockRecord1)));
    assertThat(binVariantsOutput, CoreMatchers.hasItem(KV.of(KV.of("chr7", 200000L), blockRecord2)));
    assertEquals(8, binVariantsOutput.size());
  }

  @Test
  public void testBinShuffleAndCombine() {

    Pipeline p = TestPipeline.create();

    PCollection<Variant> mergedVariants = p.apply(Create.of(input))
        .apply(new JoinNonVariantSegmentsWithVariants.BinShuffleAndCombineTransform());

    DataflowAssert.that(mergedVariants).satisfies(
        new AssertThatHasExpectedContentsForTestJoinVariants());

    p.run();
  }

  static class AssertThatHasExpectedContentsForTestJoinVariants implements
      SerializableFunction<Iterable<Variant>, Void> {

    @Override
    public Void apply(Iterable<Variant> actual) {
      assertThat(actual, CoreMatchers.hasItem(expectedSnp1));
      assertThat(actual, CoreMatchers.hasItem(expectedSnp2));
      assertThat(actual, CoreMatchers.hasItem(expectedInsert));
      assertThat(actual, IsIterableWithSize.<Variant>iterableWithSize(3));

      return null;
    }
  }
}
