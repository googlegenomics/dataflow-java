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
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.hamcrest.CoreMatchers;
import org.hamcrest.collection.IsIterableWithSize;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.functions.JoinGvcfVariants.VariantComparator;
import com.google.cloud.genomics.dataflow.utils.DataUtils;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;

@RunWith(JUnit4.class)
public class JoinGvcfVariantsTest {

  private static final Call[] variantCalls = new Call[] {
      DataUtils.makeCall("het-alt sample", 1, 0), DataUtils.makeCall("hom-alt sample", 1, 1)};

  private static final Call[] blockRecord1Calls = new Call[] {
      DataUtils.makeCall("hom sample", 0, 0), DataUtils.makeCall("no call sample", -1, -1)};

  private static final Call[] blockRecord2Calls = new Call[] {DataUtils.makeCall(
      "hom no-call sample", -1, 0)};

  private static final Variant expectedSnp1 = DataUtils.makeVariant("chr7", 200010, 200011, "A",
      newArrayList("C"), variantCalls[0], variantCalls[1], blockRecord1Calls[0],
      blockRecord1Calls[1]);

  private static final Variant expectedSnp2 = DataUtils.makeVariant("chr7", 200019, 200020, "T",
      newArrayList("G"), variantCalls[0], variantCalls[1], blockRecord1Calls[0],
      blockRecord1Calls[1], blockRecord2Calls[0]);

  private static final Variant expectedInsert = DataUtils.makeVariant("chr7", 200010, 200011, "A",
      newArrayList("AC"), variantCalls);

  private Variant snp1;
  private Variant snp2;
  private Variant insert;
  private Variant blockRecord1;
  private Variant blockRecord2;
  private Variant[] input;

  @Before
  public void setUp() {
    snp1 = DataUtils.makeVariant("chr7", 200010, 200011, "A", newArrayList("C"), variantCalls);

    snp2 = DataUtils.makeVariant("chr7", 200019, 200020, "T", newArrayList("G"), variantCalls);

    insert = DataUtils.makeVariant("chr7", 200010, 200011, "A", newArrayList("AC"), variantCalls);

    blockRecord1 = DataUtils.makeVariant("chr7", 199005, 202050, "A", null, blockRecord1Calls);

    blockRecord2 = DataUtils.makeVariant("chr7", 200011, 200020, "A", null, blockRecord2Calls);

    input = new Variant[] {snp1, snp2, insert, blockRecord1, blockRecord2};
  }

  @Test
  public void testVariantComparator() {
    assertEquals(-1, new VariantComparator().compare(blockRecord1, snp1));
    assertEquals(1, new VariantComparator().compare(blockRecord2, snp1));

    // Two variants at the same location
    assertEquals(0, new VariantComparator().compare(
        DataUtils.makeVariant("chr7", 200010, 200011, "A", newArrayList("C"), (Call[]) null),
        DataUtils.makeVariant("chr7", 200010, 200011, "A", newArrayList("T"), (Call[]) null)));

    // Block record and variant at the same location
    assertEquals(1, new VariantComparator().compare(
        DataUtils.makeVariant("chr7", 200010, 200011, "A", newArrayList("C"), (Call[]) null),
        DataUtils.makeVariant("chr7", 200010, 200011, "A", null, (Call[]) null)));
  }

  @Test
  public void testIsOverlapping() {
    assertTrue(JoinGvcfVariants.isOverlapping(blockRecord1, snp1));
    assertTrue(JoinGvcfVariants.isOverlapping(blockRecord1, snp2));
    assertFalse(JoinGvcfVariants.isOverlapping(blockRecord2, snp1));
    assertTrue(JoinGvcfVariants.isOverlapping(blockRecord2, snp2));
  }

  @Test
  public void testBinVariantsFn() {

    DoFnTester<Variant, KV<KV<String, Long>, Variant>> binVariantsFn =
        DoFnTester.of(new JoinGvcfVariants.BinVariants());

    List<KV<KV<String, Long>, Variant>> binVariantsOutput = binVariantsFn.processBatch(input);
    Assert.assertThat(binVariantsOutput, CoreMatchers.hasItem(KV.of(KV.of("chr7", 200L), snp1)));
    Assert.assertThat(binVariantsOutput, CoreMatchers.hasItem(KV.of(KV.of("chr7", 200L), snp2)));
    Assert.assertThat(binVariantsOutput, CoreMatchers.hasItem(KV.of(KV.of("chr7", 200L), insert)));
    Assert.assertThat(binVariantsOutput,
        CoreMatchers.hasItem(KV.of(KV.of("chr7", 199L), blockRecord1)));
    Assert.assertThat(binVariantsOutput,
        CoreMatchers.hasItem(KV.of(KV.of("chr7", 200L), blockRecord1)));
    Assert.assertThat(binVariantsOutput,
        CoreMatchers.hasItem(KV.of(KV.of("chr7", 201L), blockRecord1)));
    Assert.assertThat(binVariantsOutput,
        CoreMatchers.hasItem(KV.of(KV.of("chr7", 202L), blockRecord1)));
    Assert.assertThat(binVariantsOutput,
        CoreMatchers.hasItem(KV.of(KV.of("chr7", 200L), blockRecord2)));
    assertEquals(8, binVariantsOutput.size());
  }

  @Test
  public void testJoinGvcfPipeline() {

    Pipeline p = TestPipeline.create();
    DataflowWorkarounds.registerGenomicsCoders(p);

    PCollection<Variant> inputVariants = p.apply(Create.of(input));

    PCollection<KV<KV<String, Long>, Variant>> binnedVariants =
        inputVariants.apply(ParDo.of(new JoinGvcfVariants.BinVariants()));

    // TODO check that windowing function is not splitting these groups across different windows
    PCollection<KV<KV<String, Long>, Iterable<Variant>>> groupedBinnedVariants =
        binnedVariants.apply(GroupByKey.<KV<String, Long>, Variant>create());

    PCollection<Variant> mergedVariants =
        groupedBinnedVariants.apply(ParDo.of(new JoinGvcfVariants.MergeVariants()));

    DataflowAssert.that(mergedVariants).satisfies(
        new AssertThatHasExpectedContentsForTestJoinGvcf());

    p.run();
  }

  @SuppressWarnings("serial")
  static class AssertThatHasExpectedContentsForTestJoinGvcf implements
      SerializableFunction<Iterable<Variant>, Void> {

    @Override
    public Void apply(Iterable<Variant> actual) {
      Assert.assertThat(actual, CoreMatchers.hasItem(expectedSnp1));
      Assert.assertThat(actual, CoreMatchers.hasItem(expectedSnp2));
      Assert.assertThat(actual, CoreMatchers.hasItem(expectedInsert));
      Assert.assertThat(actual, IsIterableWithSize.<Variant>iterableWithSize(3));

      return null;
    }
  }
}
