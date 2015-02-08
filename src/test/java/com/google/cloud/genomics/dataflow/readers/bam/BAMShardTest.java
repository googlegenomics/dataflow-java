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
