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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.genomics.dataflow.utils.DataUtils;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

@RunWith(JUnit4.class)
public class ExtractSimilarCallsetsTest {

 
  @Test
  public void testGetSamplesWithVariant() throws Exception {
    
    BiMap<String, Integer> dataIndices = HashBiMap.create();
    dataIndices.put("ref", dataIndices.size());
    dataIndices.put("alt1", dataIndices.size());
    dataIndices.put("alt2", dataIndices.size());
    dataIndices.put("alt3", dataIndices.size());

    ExtractSimilarCallsets doFn = new ExtractSimilarCallsets(dataIndices);
    
    Variant variant = new Variant();
    List<Call> calls = new ArrayList<Call>();

    variant.setCalls(calls);
    assertEquals(Collections.emptyList(), doFn.getSamplesWithVariant(variant));

    calls.add(DataUtils.makeCall("ref", 0, 0));
    assertEquals(Collections.emptyList(), doFn.getSamplesWithVariant(variant));

    calls.add(DataUtils.makeCall("alt1", 1, 0));
    assertEquals(Collections.singletonList(1), doFn.getSamplesWithVariant(variant));

    calls.add(DataUtils.makeCall("alt2", 0, 1));
    calls.add(DataUtils.makeCall("alt3", 1, 1));
    assertEquals(Arrays.asList(1, 2, 3), doFn.getSamplesWithVariant(variant));
  }

}
