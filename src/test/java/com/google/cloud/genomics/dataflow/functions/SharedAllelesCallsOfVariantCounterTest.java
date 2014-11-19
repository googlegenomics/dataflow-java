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
package com.google.cloud.genomics.dataflow.functions;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.api.services.genomics.model.Call;

import static com.google.cloud.genomics.dataflow.functions.SharedAllelesCounter.ratioOfSharedAlleles;

@RunWith(JUnit4.class)
public class SharedAllelesCallsOfVariantCounterTest {

  private static final double DELTA = 1e-6;

  @Test
  public void testRatioOfSharedAlleles() {
    List<Call> calls = new ArrayList<Call>();

    calls.add(makeCall("ref", 0, 0));
    calls.add(makeCall("alt1", 1, 0));
    calls.add(makeCall("alt2", 0, 1));
    calls.add(makeCall("alt3", 1, 1));
    calls.add(makeCall("alt4", 1, 1));
    calls.add(makeCall("alt5", 1));
    calls.add(makeCall("alt6", 0));
    calls.add(makeCall("alt7", 1, 0, 1));
    calls.add(makeCall("alt8", 1, 0, 0));

    assertEquals(0.5, ratioOfSharedAlleles(calls.get(0), calls.get(1)), DELTA);
    assertEquals(0, ratioOfSharedAlleles(calls.get(0), calls.get(4)), DELTA);
    assertEquals(1, ratioOfSharedAlleles(calls.get(3), calls.get(4)), DELTA);
    assertEquals(1, ratioOfSharedAlleles(calls.get(3), calls.get(4)), DELTA);
    assertEquals(0.5, ratioOfSharedAlleles(calls.get(4), calls.get(5)), DELTA);
    assertEquals(1.0 / 3.0, ratioOfSharedAlleles(calls.get(5), calls.get(7)), DELTA);
    assertEquals(2.0 / 3.0, ratioOfSharedAlleles(calls.get(7), calls.get(8)), DELTA);
    assertEquals(0, ratioOfSharedAlleles(calls.get(6), calls.get(7)), DELTA);
  }

  private static Call makeCall(String name, Integer... alleles) {
    return new Call().setCallSetName(name).setGenotype(Arrays.asList(alleles));
  }
}
