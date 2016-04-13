/*
 * Copyright 2015 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.genomics.dataflow.functions;

import com.google.cloud.genomics.dataflow.model.ReadCounts;
import com.google.cloud.genomics.dataflow.model.ReadQualityCount;
import com.google.cloud.genomics.dataflow.model.ReadQualityCount.Base;
import com.google.common.collect.ImmutableMap;
import com.google.genomics.v1.Position;

import junit.framework.TestCase;

import org.apache.commons.math3.util.Precision;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Map;

/**
 * Unit tests for {@link LikelihoodFn}.
 */
@RunWith(JUnit4.class)
public class LikelihoodFnTest extends TestCase {

  @Test
  public void testValue() {
    final double tolerance = 0.0000001;  // slop factor for floating point comparisons
    final double alpha = 0.25;
    final int errPhred = 10;  // P(err) = 0.1
    final double pRef = 0.6;

    Position position1 = Position.newBuilder()
        .setReferenceName("1")
        .setPosition(123L)
        .build();

    /*
     * Observe single REF read
     */
    ImmutableMap.Builder<Position, ReadCounts> countsBuilder
      = ImmutableMap.builder();
    ReadCounts rc = new ReadCounts();
    rc.setRefFreq(pRef);
    ReadQualityCount rqc = new ReadQualityCount();
    rqc.setBase(Base.REF);
    rqc.setQuality(errPhred);
    rqc.setCount(1);
    rc.addReadQualityCount(rqc);
    countsBuilder.put(position1, rc);
    Map<Position, ReadCounts> readCounts = countsBuilder.build();
    LikelihoodFn fn = new LikelihoodFn(readCounts);
    // Likelihood of a single REF with the parameters above
    final double likRef = 0.3354133333;
    assertEquals(Precision.compareTo(fn.value(alpha), Math.log(likRef), tolerance), 0);

    /*
     * Observe single NONREF read
     */
    countsBuilder = ImmutableMap.builder();
    rc = new ReadCounts();
    rc.setRefFreq(pRef);
    rqc = new ReadQualityCount();
    rqc.setBase(Base.NONREF);
    rqc.setQuality(errPhred);
    rqc.setCount(1);
    rc.addReadQualityCount(rqc);
    countsBuilder.put(position1, rc);
    readCounts = countsBuilder.build();
    fn = new LikelihoodFn(readCounts);
    // Likelihood of a single NONREF with the parameters above
    final double likNonref = 0.20368;
    assertEquals(Precision.compareTo(fn.value(alpha), Math.log(likNonref), tolerance), 0);
  
    /*
     * Observe single OTHER read
     */
    countsBuilder = ImmutableMap.builder();
    rc = new ReadCounts();
    rc.setRefFreq(pRef);
    rqc = new ReadQualityCount();
    rqc.setBase(Base.OTHER);
    rqc.setQuality(errPhred);
    rqc.setCount(1);
    rc.addReadQualityCount(rqc);
    countsBuilder.put(position1, rc);
    readCounts = countsBuilder.build();
    fn = new LikelihoodFn(readCounts);
    // Likelihood of a single OTHER with the parameters above
    final double likOther = 0.03850666667;
    assertEquals(Precision.compareTo(fn.value(alpha), Math.log(likOther), tolerance), 0);

    // Likelihood for reads at 2 different positions should be product of
    // likelihoods for individual reads
    Position position2 = Position.newBuilder()
        .setReferenceName("1")
        .setPosition(124L)
        .build();
    countsBuilder = ImmutableMap.builder();
    rc = new ReadCounts();
    rc.setRefFreq(pRef);
    rqc = new ReadQualityCount();
    rqc.setBase(Base.REF);
    rqc.setQuality(errPhred);
    rqc.setCount(1);
    rc.addReadQualityCount(rqc);
    countsBuilder.put(position1, rc);
    rc = new ReadCounts();
    rc.setRefFreq(pRef);
    rqc = new ReadQualityCount();
    rqc.setBase(Base.NONREF);
    rqc.setQuality(errPhred);
    rqc.setCount(1);
    rc.addReadQualityCount(rqc);
    countsBuilder.put(position2, rc);
    readCounts = countsBuilder.build();
    fn = new LikelihoodFn(readCounts);
    assertEquals(Precision.compareTo(fn.value(alpha), Math.log(likRef * likNonref),
        tolerance), 0);
  }
}
