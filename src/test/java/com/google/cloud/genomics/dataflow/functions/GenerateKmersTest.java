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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class GenerateKmersTest {

  @Test
  public void testGetKmers() throws Exception {
    String seq = "ABCNCBA";
    int k = 3;
    GenerateKmers fn = new GenerateKmers(k);
    List<String> kmers = fn.getKmers(seq);
    
    assertTrue(kmers.size() == 2);
    assertTrue(kmers.contains("ABC"));
    assertTrue(kmers.contains("CBA"));
    assertTrue(!kmers.contains("BCN"));
    assertTrue(!kmers.contains("CNC"));
    assertTrue(!kmers.contains("NCB"));
  }
}