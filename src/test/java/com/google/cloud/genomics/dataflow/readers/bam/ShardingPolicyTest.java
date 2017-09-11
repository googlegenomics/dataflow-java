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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import htsjdk.samtools.Chunk;

import java.util.Collections;

@RunWith(JUnit4.class)
public class ShardingPolicyTest {
  @Test
  public void testShardPolicyBytes() {
    BAMShard shard = new BAMShard("f","chr20",1);
    Chunk chunk = new Chunk(1,11L*1024L*1024L << 16);
    shard.addBin(Collections.singletonList(chunk),90000);
    Assert.assertTrue(
    		"Shard of size " + shard.approximateSizeInBytes() +
    		" is NOT big enough but it should be",
    		ShardingPolicy.BYTE_SIZE_POLICY_10MB.apply(shard));

    shard = new BAMShard("f","chr20",1);
    chunk = new Chunk(1,1L*1024L*1024L << 16);
    shard.addBin(Collections.singletonList(chunk),90000);
    Assert.assertFalse(
    		"Shard of size " + shard.approximateSizeInBytes() +
    		" is big enough but it should NOT be",
    		ShardingPolicy.BYTE_SIZE_POLICY_10MB.apply(shard));
  }

  @Test
  public void testShardPolicyLoci() {
    BAMShard shard = new BAMShard("f","chr20",1);
    Chunk chunk = new Chunk(1,9*1024*1024);
    shard.addBin(Collections.singletonList(chunk),110000);
    Assert.assertTrue(
    		"Shard of size " + shard.sizeInLoci() +
    		" is NOT big enough but it should be",
    		ShardingPolicy.LOCI_SIZE_POLICY_100KBP.apply(shard));

    shard = new BAMShard("f","chr20",1);
    chunk = new Chunk(1,9*1024*1024);
    shard.addBin(Collections.singletonList(chunk),90000);
    Assert.assertFalse(
    		"Shard of size " + shard.sizeInLoci() +
    		" is big enough but it should NOT be",
    		ShardingPolicy.LOCI_SIZE_POLICY_100KBP.apply(shard));
  }
}
