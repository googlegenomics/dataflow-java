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

import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.functions.CreateKmerTable.GenTable;
import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;


@RunWith(JUnit4.class)
public class CreateKmerTableTest {
  
  @Test
  public void testGenTable() {
    KV<KV<String, String>, Long> record1 = KV.of(KV.of("Row1", "AA"), 5L);
    KV<KV<String, String>, Long> record2 = KV.of(KV.of("Row1", "AT"), 6L);
    KV<KV<String, String>, Long> record3 = KV.of(KV.of("Row2", "AA"), 7L);
    KV<KV<String, String>, Long> record4 = KV.of(KV.of("Row2", "AT"), 8L);
    
    Iterable<String> result = new GenTable().apply(
        Lists.newArrayList(record1, record2, record3, record4));
    
    ArrayList<String> rows = new ArrayList<String>();
    for (String row : result) {
      rows.add(row);
    }
    
    assertTrue(rows.contains("Accessions,AA,AT") || rows.contains("Accessions,AT,AA"));
    if (rows.contains("Accessions,AA,AT")) {
      assertTrue(rows.contains("Row1,5,6"));
      assertTrue(rows.contains("Row2,7,8"));
    } else {
      assertTrue(rows.contains("Row1,6,5"));
      assertTrue(rows.contains("Row2,8,7"));
    }
  }
}