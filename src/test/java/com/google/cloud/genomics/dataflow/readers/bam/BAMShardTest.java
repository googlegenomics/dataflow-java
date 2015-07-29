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
package com.google.cloud.genomics.dataflow.readers.bam;

import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.Test;

import htsjdk.samtools.BAMFileIndexImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import htsjdk.samtools.Chunk;

@RunWith(JUnit4.class)
public class BAMShardTest {
  @Test
  public void testShardGrowth() {
    BAMShard shard = new BAMShard("f","chr20",1);
    Chunk chunk = new Chunk(1,20);
    shard.addBin(Collections.singletonList(chunk),10);
    assertEquals(9, shard.sizeInLoci());
    assertEquals(19, shard.approximateSizeInBytes());
  }
}
