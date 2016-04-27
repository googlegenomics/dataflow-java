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
package com.google.cloud.genomics.dataflow.functions.verifybamid;

import com.google.cloud.genomics.dataflow.model.ReadCounts;
import com.google.cloud.genomics.dataflow.model.ReadQualityCount;
import com.google.cloud.genomics.dataflow.model.ReadQualityCount.Base;
import com.google.common.collect.ImmutableMap;
import com.google.genomics.v1.Position;

import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.geometry.euclidean.oned.Interval;
import org.apache.commons.math3.util.Precision;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Map;

import junit.framework.TestCase;

/**
 * Unit tests for Solver.
 */
@RunWith(JUnit4.class)
public class SolverTest extends TestCase {

  /** Parabola with maximum value at a specified point */
  public class Parabola implements UnivariateFunction {
    private final double max;

    /** Create a Parabola with maximum at the specified value */
    Parabola(double max) {
      this.max = max;
    }

    @Override
    public double value(double x) {
      return 1.0 - Math.pow(x - this.max, 2.0);
    }
  }

  @Test
  public void testGridSearch() {
    Interval interval = Solver.gridSearch(new Parabola(0.25), 0.0, 1.0, 0.1);
    assertEquals(Precision.compareTo(interval.getInf(), 0.1, 1), 0);
    assertEquals(Precision.compareTo(interval.getSup(), 0.3, 1), 0);

    interval = Solver.gridSearch(new Parabola(1.2), 0.0, 1.0, 0.1);
    assertEquals(Precision.compareTo(interval.getInf(), 0.9, 1), 0);
    assertEquals(Precision.compareTo(interval.getSup(), 1.0, 1), 0);

    interval = Solver.gridSearch(new Parabola(1.2), 0.0, 1.0, 0.3);
    assertEquals(Precision.compareTo(interval.getInf(), 0.9, 1), 0);
    assertEquals(Precision.compareTo(interval.getSup(), 1.0, 1), 0);
  }

  @Test
  public void testMaximize() {
    assertEquals(Precision.compareTo(Solver.maximize(new Parabola(0.47), 0.0, 1.0,
        0.1, 0.00001, 0.00001, 100, 100), 0.47, 0.00001), 0);
  }

  @Test
  public void testSolverOnKnownLikelihoodCases() {
    int phred = 200;

    Position position1 = Position.newBuilder()
        .setReferenceName("1")
        .setPosition(123L)
        .build();

    /*
     * Observe 900 REF reads and 100 NONREF
     * P(REF) = 0.8
     * error probability is near 0
     * Most likely explanation should be ~20% contamination
     * (if P(REF) were 0.5, we'd have peaks at 10% (nonref homozygous contaminant)
     * and 20% (heterozygous contaminant)
     */
    ImmutableMap.Builder<Position, ReadCounts> countsBuilder
      = ImmutableMap.builder();
    ReadCounts rc = new ReadCounts();
    rc.setRefFreq(0.8);
    ReadQualityCount rqc = new ReadQualityCount();
    rqc.setBase(Base.REF);
    rqc.setQuality(phred);
    rqc.setCount(900);
    rc.addReadQualityCount(rqc);
    rqc = new ReadQualityCount();
    rqc.setBase(Base.NONREF);
    rqc.setQuality(phred);
    rqc.setCount(100);
    rc.addReadQualityCount(rqc);
    rqc = new ReadQualityCount();
    rqc.setBase(Base.OTHER);
    rqc.setQuality(1);
    rqc.setCount(2);
    rc.addReadQualityCount(rqc);
    countsBuilder.put(position1, rc);
    Map<Position, ReadCounts> readCounts = countsBuilder.build();
    assertEquals(Precision.compareTo(Solver.maximize(
        new LikelihoodFn(readCounts), 0.0, 0.5, 0.05,
        0.0001, 0.0001, 100, 100), 0.2, 0.0001), 0);

    /*
     * Make sure things are symmetrical.  Observe 900 NONREF reads and 100 REF
     * P(NONREF) = 0.8 (i.e. P(REF) = 0.2)
     * error probability is near 0
     * Most likely explanation should be ~20% contamination
     * (if P(REF) were 0.5, we'd have peaks at 10% (nonref homozygous contaminant)
     * and 20% (heterozygous contaminant)
     */
    countsBuilder = ImmutableMap.builder();
    rc = new ReadCounts();
    rc.setRefFreq(0.2);
    rqc = new ReadQualityCount();
    rqc.setBase(Base.NONREF);
    rqc.setQuality(phred);
    rqc.setCount(900);
    rc.addReadQualityCount(rqc);
    rqc = new ReadQualityCount();
    rqc.setBase(Base.REF);
    rqc.setQuality(phred);
    rqc.setCount(100);
    rc.addReadQualityCount(rqc);
    countsBuilder.put(position1, rc);
    readCounts = countsBuilder.build();
    assertEquals(Precision.compareTo(Solver.maximize(
        new LikelihoodFn(readCounts), 0.0, 0.5, 0.05,
        0.0001, 0.0001, 100, 100), 0.2, 0.0001), 0);

    /*
     * Assume a heterozygous desired base pair with a homozygous contaminant.
     * Observe 450 NONREF reads and 550 REF
     * P(REF) = 0.8
     * error probability is near 0
     * Most likely explanation should be ~10% contamination
     */
    countsBuilder = ImmutableMap.builder();
    rc = new ReadCounts();
    rc.setRefFreq(0.5);
    rqc = new ReadQualityCount();
    rqc.setBase(Base.NONREF);
    rqc.setQuality(phred);
    rqc.setCount(450);
    rc.addReadQualityCount(rqc);
    rqc = new ReadQualityCount();
    rqc.setBase(Base.REF);
    rqc.setQuality(phred);
    rqc.setCount(550);
    rc.addReadQualityCount(rqc);
    countsBuilder.put(position1, rc);
    readCounts = countsBuilder.build();
    assertEquals(Precision.compareTo(Solver.maximize(
        new LikelihoodFn(readCounts), 0.0, 0.5, 0.05,
        0.0001, 0.0001, 100, 100), 0.1, 0.0001), 0);
  }

}