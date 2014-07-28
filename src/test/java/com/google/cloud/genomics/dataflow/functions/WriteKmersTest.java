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

import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.functions.WriteKmers.FormatKmer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class WriteKmersTest {
  
  @Test
  public void testFormatKmers() {
    DoFnTester<KV<KV<String, String>, Long>, String> tester = DoFnTester.of(new FormatKmer());
    @SuppressWarnings("unchecked")
    List<String> output = tester.processBatch(KV.of(KV.of("TEST", "STRING"), 1234L));
    assertTrue(output.size() == 1);
    assertTrue(output.contains("TEST-STRING-1234:"));
  }
}