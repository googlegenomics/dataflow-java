package com.google.cloud.genomics.dataflow.readers.bam;

import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import htsjdk.samtools.Chunk;

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
    		ShardingPolicy.BYTE_SIZE_POLICY.shardBigEnough(shard));
    
    shard = new BAMShard("f","chr20",1);
    chunk = new Chunk(1,1L*1024L*1024L << 16);
    shard.addBin(Collections.singletonList(chunk),90000);
    Assert.assertFalse(
    		"Shard of size " + shard.approximateSizeInBytes() + 
    		" is big enough but it should NOT be",
    		ShardingPolicy.BYTE_SIZE_POLICY.shardBigEnough(shard));
  }
  
  @Test
  public void testShardPolicyLoci() {
    BAMShard shard = new BAMShard("f","chr20",1);
    Chunk chunk = new Chunk(1,9*1024*1024);
    shard.addBin(Collections.singletonList(chunk),110000);
    Assert.assertTrue(
    		"Shard of size " + shard.sizeInLoci() + 
    		" is NOT big enough but it should be",
    		ShardingPolicy.LOCI_SIZE_POLICY.shardBigEnough(shard));
    
    shard = new BAMShard("f","chr20",1);
    chunk = new Chunk(1,9*1024*1024);
    shard.addBin(Collections.singletonList(chunk),90000);
    Assert.assertFalse(
    		"Shard of size " + shard.sizeInLoci() + 
    		" is big enough but it should NOT be",
    		ShardingPolicy.LOCI_SIZE_POLICY.shardBigEnough(shard));
  }
}
