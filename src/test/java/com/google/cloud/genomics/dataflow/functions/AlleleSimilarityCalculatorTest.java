package com.google.cloud.genomics.dataflow.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.utils.DataUtils;

public class AlleleSimilarityCalculatorTest {

  static final Variant snp1 = DataUtils.makeVariant("chr7", 200019, 200020, "T", Collections.singletonList("G"),
      DataUtils.makeCall("het-alt sample", 1, 0), DataUtils.makeCall("hom-alt sample", 1, 1),
      DataUtils.makeCall("hom-ref sample", 0, 0), DataUtils.makeCall("hom-nocall sample", -1, -1),
      DataUtils.makeCall("ref-nocall sample", -1, 0));

  static final Variant snp2 = DataUtils.makeVariant("chr7", 200020, 200021, "C", Collections.singletonList("A"),
      DataUtils.makeCall("hom-alt sample", 1, 1), DataUtils.makeCall("het-alt sample", 0, 1),
      DataUtils.makeCall("ref-nocall sample", 0, -1));

  @Test
  public void testIsReferenceMajor() {
    assertTrue(AlleleSimilarityCalculator.isReferenceMajor(snp1));
    assertFalse(AlleleSimilarityCalculator.isReferenceMajor(snp2));
  }

  @Test
  public void testGetSamplesWithVariant() {
    assertEquals(4, AlleleSimilarityCalculator.getSamplesWithVariant(snp1).size());
    assertEquals(3, AlleleSimilarityCalculator.getSamplesWithVariant(snp2).size());
  }

  @Test
  public void testSharedMinorAllSimilarityFn() {

    CallSimilarityCalculatorFactory fac = new SharedMinorAllelesCalculatorFactory();
    DoFnTester<Variant, KV<KV<String, String>, KV<Double, Integer>>> simFn =
        DoFnTester.of(new AlleleSimilarityCalculator(fac));

    List<KV<KV<String, String>, KV<Double, Integer>>> outputSnp1 = simFn.processBatch(snp1);
    assertEquals(6, outputSnp1.size());
    assertThat(outputSnp1, CoreMatchers.hasItems(
        KV.of(KV.of("het-alt sample", "ref-nocall sample"), KV.of(0.0, 1)),
        KV.of(KV.of("het-alt sample", "hom-ref sample"), KV.of(0.0, 1)),
        KV.of(KV.of("het-alt sample", "hom-alt sample"), KV.of(1.0, 1)),
        KV.of(KV.of("hom-ref sample", "ref-nocall sample"), KV.of(0.0, 1)),
        KV.of(KV.of("hom-alt sample", "ref-nocall sample"), KV.of(0.0, 1)),
        KV.of(KV.of("hom-alt sample", "hom-ref sample"), KV.of(0.0, 1))));

    List<KV<KV<String, String>, KV<Double, Integer>>> outputSnp2 = simFn.processBatch(snp2);
    assertEquals(3, outputSnp2.size());
    assertThat(
        outputSnp2,
        CoreMatchers.hasItems(KV.of(KV.of("het-alt sample", "hom-alt sample"), KV.of(0.0, 1)),
            KV.of(KV.of("het-alt sample", "ref-nocall sample"), KV.of(1.0, 1)),
            KV.of(KV.of("hom-alt sample", "ref-nocall sample"), KV.of(0.0, 1))));

    Variant[] input = new Variant[] {snp1, snp2};
    List<KV<KV<String, String>, KV<Double, Integer>>> outputBoth = simFn.processBatch(input);
    assertEquals(6, outputBoth.size());
    assertThat(outputBoth, CoreMatchers.hasItems(
        KV.of(KV.of("het-alt sample", "ref-nocall sample"), KV.of(1.0, 2)),
        KV.of(KV.of("het-alt sample", "hom-ref sample"), KV.of(0.0, 1)),
        KV.of(KV.of("het-alt sample", "hom-alt sample"), KV.of(1.0, 2)),
        KV.of(KV.of("hom-ref sample", "ref-nocall sample"), KV.of(0.0, 1)),
        KV.of(KV.of("hom-alt sample", "ref-nocall sample"), KV.of(0.0, 2)),
        KV.of(KV.of("hom-alt sample", "hom-ref sample"), KV.of(0.0, 1))));

  }
}
