/*
 * Copyright (C) 2015 Google Inc.
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

import com.google.cloud.genomics.utils.Contig;
import com.google.common.collect.Lists;

import htsjdk.samtools.BAMFileIndexImpl;
import htsjdk.samtools.Chunk;
import htsjdk.samtools.SAMFileSpanImpl;

import java.io.Serializable;
import java.util.List;

/**
 * A shard of BAM data we will create during sharding and then use to drive the reading.
 * We use this class during shard generation, by iteratively building 
 * a shard, extending it bin by bin (@see #addBin)
 * At the end of the process, the shard is finalized (@see #finalize) 
 * and SAMFileSpan that has all the chunks we want to read is produced.
 */
public class BAMShard implements Serializable {
  public String file;
  public SAMFileSpanImpl span;
  public Contig contig;
  public List<Chunk> chunks;
  public long cachedSizeInBytes = -1;

  /**
   * Begins a new shard with an empty chunk list and a starting locus.
   */
  public BAMShard(String file, String referenceName, long firstLocus) {
    this.file = file;
    this.contig = new Contig(referenceName, firstLocus, -1);
    this.chunks = Lists.newLinkedList();
    this.span = null;
  }
  
  /**
   * Creates a shard with a known file span.
   * Such shard is not expected to be extended and calling addBin or finalize on it will fail.
   * This constructor is used for "degenerate" shards like unmapped reads or 
   * all reads in cases where we don't have an index.
   */
  public BAMShard(String file, SAMFileSpanImpl span, Contig contig) {
    this.file = file;
    this.span = span;
    this.contig = contig;
    this.chunks = null;
  }

  /**
   * Appends chunks from another bin to the list and moved the end position.
   */
  public void addBin(List<Chunk> chunksToAdd, long lastLocus) {
    assert chunks != null;
    contig = new Contig(contig.referenceName, contig.start, lastLocus);
    chunks.addAll(chunksToAdd);
    updateSpan();
  }
  
  /**
   * Generates a final list of chunks, now that we know the exact bounding loci
   * for this shard. We get all chunks overlapping this loci, and then ask the index
   * for the chunks overlapping them. 
   */
  public BAMShard finalize(BAMFileIndexImpl index, int lastLocus) {
    contig = new Contig(contig.referenceName, contig.start, lastLocus);
    this.chunks = index.getChunksOverlapping(contig.referenceName, 
        (int)contig.start, (int)contig.end);
    updateSpan();
    return this;
  }
  
  /**
   * Updates the underlying file span by optimizing and coalescing the current chunk list.
   */
  private void updateSpan() {
    span = new SAMFileSpanImpl(Chunk.optimizeChunkList(chunks, this.contig.start));
    cachedSizeInBytes = -1;
  }

  public long sizeInLoci() {
    return contig.end > 0 ? contig.end - contig.start : 0;
  }
  
  public long approximateSizeInBytes() {
    if (cachedSizeInBytes < 0) {
      cachedSizeInBytes = span.approximateSizeInBytes();
    }
    return cachedSizeInBytes;
  }

  @Override
  public String toString() {
    return file + ": " + contig.toString() + ", locus size = " + sizeInLoci() + 
        ", span size = " + approximateSizeInBytes();
  }
}