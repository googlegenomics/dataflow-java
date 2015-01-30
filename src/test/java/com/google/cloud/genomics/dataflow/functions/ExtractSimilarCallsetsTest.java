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

import static com.google.cloud.genomics.dataflow.functions.ExtractSimilarCallsets.getSamplesWithVariant;
import static org.junit.Assert.assertEquals;

import com.google.api.client.util.Lists;
import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.utils.PairGenerator;
import com.google.cloud.genomics.dataflow.utils.DataUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@RunWith(JUnit4.class)
public class ExtractSimilarCallsetsTest {

 
  @Test
  public void testGetSamplesWithVariant() throws Exception {
    Variant variant = new Variant();
    List<Call> calls = new ArrayList<Call>();

    variant.setCalls(calls);
    assertEquals(Collections.emptyList(), getSamplesWithVariant(variant));

    calls.add(DataUtils.makeCall("ref", 0, 0));
    assertEquals(Collections.emptyList(), getSamplesWithVariant(variant));

    calls.add(DataUtils.makeCall("alt1", 1, 0));
    assertEquals(Collections.singletonList("alt1"), getSamplesWithVariant(variant));

    calls.add(DataUtils.makeCall("alt2", 0, 1));
    calls.add(DataUtils.makeCall("alt3", 1, 1));
    assertEquals(Arrays.asList("alt1", "alt2", "alt3"), getSamplesWithVariant(variant));
  }

}
