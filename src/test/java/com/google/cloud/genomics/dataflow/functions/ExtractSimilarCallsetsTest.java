/*
Copyright 2014 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.google.cloud.genomics.dataflow.functions;

import static org.junit.Assert.assertEquals;

import com.google.api.client.util.Lists;
import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

@RunWith(JUnit4.class)
public class ExtractSimilarCallsetsTest {

  @Test
  public void testGetSamplesWithVariant() throws Exception {
    Call refCall = makeCall("ref", 0, 0);
    Call altCall1 = makeCall("alt1", 1, 0);
    Call altCall2 = makeCall("alt2", 0, 1);
    Call altCall3 = makeCall("alt3", 1, 1);

    ExtractSimilarCallsets fn = new ExtractSimilarCallsets();
    Variant variant = new Variant();

    variant.setCalls(Lists.<Call>newArrayList());
    assertEquals(0, fn.getSamplesWithVariant(variant).size());

    variant.getCalls().add(refCall);
    assertEquals(0, fn.getSamplesWithVariant(variant).size());

    variant.getCalls().add(altCall1);
    assertEquals(1, fn.getSamplesWithVariant(variant).size());

    variant.getCalls().add(altCall2);
    variant.getCalls().add(altCall3);
    List<String> samples = fn.getSamplesWithVariant(variant);
    assertEquals(3, samples.size());
    assertEquals("alt1", samples.get(0));
    assertEquals("alt2", samples.get(1));
    assertEquals("alt3", samples.get(2));
  }

  private Call makeCall(String name,
      Integer firstAllele,
      Integer secondAllele) {
    List<Integer> genotype = Lists.newArrayList();
    genotype.add(firstAllele);
    genotype.add(secondAllele);

    Call call1 = new Call();
    call1.setCallSetName(name);
    call1.setGenotype(genotype);
    return call1;
  }

}