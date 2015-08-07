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

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.genomics.utils.Contig;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import htsjdk.samtools.BAMFileIndexImpl;
import htsjdk.samtools.Bin;
import htsjdk.samtools.Chunk;
import htsjdk.samtools.GenomicIndexUtil;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.IOUtil;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * Generates shards for a given BAM file and a desired set of contigs.
 */
public class Sharder {
  private static final Logger LOG = Logger.getLogger(Sharder.class.getName());
  
  public interface SharderOutput {
    public void output(BAMShard shard);
  }
  
  Storage.Objects storageClient;
  String filePath; 
  Iterable<Contig> requestedContigs;
  SharderOutput output;
  final ShardingPolicy shardingPolicy;
  
  SamReader reader;
  SeekableStream indexStream;
  SAMFileHeader header;
  BAMFileIndexImpl index;
  
  boolean allReferences;
  boolean hasIndex;
  HashMap<String, Contig> contigsByReference;
  
  public static List<BAMShard> shardBAMFile(Objects storageClient, 
      String filePath, Iterable<Contig> requestedContigs,
      ShardingPolicy shardingPolicy) throws IOException {
    final List<BAMShard> shards = Lists.newArrayList();
    (new Sharder(storageClient, filePath, requestedContigs, shardingPolicy,
        new SharderOutput() {
          @Override
          public void output(BAMShard shard) {
            shards.add(shard);
          }
        }))
      .process();
    return shards;
  }
  
  public Sharder(Objects storageClient, String filePath, Iterable<Contig> requestedContigs,
      ShardingPolicy shardingPolicy,
      SharderOutput output) {
    super();
    this.storageClient = storageClient;
    this.filePath = filePath;
    this.shardingPolicy = shardingPolicy;
    // HTSJDK expects 1-based coordinates - need to shift by 1
    this.requestedContigs = Iterables.transform(requestedContigs,
        new Function<Contig, Contig>() {
          @Override
          @Nullable
          public Contig apply(@Nullable Contig arg0) {
            return new Contig(arg0.referenceName, arg0.start + 1, arg0.end + 1);
          }
        });
    this.output = output;
  }

  public void process() throws IOException {
    LOG.info("Processing BAM file " + filePath);
        
    openFile();
    processHeader();
 
    for (SAMSequenceRecord sequenceRecord : header.getSequenceDictionary().getSequences()) {
      final Contig contig = desiredContigForReference(sequenceRecord);
      if (contig == null) {
        continue;
      } else if (!hasIndex) {
        // No index, so create a shard over all BAM file and pass a contig restrict.
        // We will iterate over all reads for this reference and skip the ones outside contig.
        LOG.info("No index: outputting shard for " + contig);
        output.output(new BAMShard(filePath, null, contig));
        continue;
      }
      createShardsForReference(sequenceRecord, contig);
    }
    if (index != null) {
      index.close();
    }
  }
  
  void openFile() throws IOException {
    final BAMIO.ReaderAndIndex r = BAMIO.openBAMAndExposeIndex(storageClient, filePath, ValidationStringency.DEFAULT_STRINGENCY);
    reader = r.reader;
    indexStream = r.index;
    header = reader.getFileHeader();
    hasIndex = reader.hasIndex() && reader.indexing().hasBrowseableIndex();
    LOG.info("Has index = " + hasIndex);
    if (hasIndex) {
      index = new BAMFileIndexImpl(
          IOUtil.maybeBufferedSeekableStream(indexStream),header.getSequenceDictionary());
    } else {
      index = null;
    }
  }
  
  void processHeader() {
    contigsByReference = Maps.newHashMap();
    for (Contig contig : requestedContigs) {
      contigsByReference.put(contig.referenceName != null ? contig.referenceName : "", contig);
    }
    if (contigsByReference.size() == 0 || contigsByReference.containsKey("*")) {
      LOG.info("Outputting unmapped reads shard ");
      output.output(new BAMShard(filePath, null, new Contig("*", 0, -1)));
    }
    final boolean allReferences =
        contigsByReference.size() == 0 || contigsByReference.containsKey("");
    LOG.info("All references = " + allReferences);
    LOG.info("BAM has index = " + reader.hasIndex());
    LOG.info("BAM has browseable index = " + reader.indexing().hasBrowseableIndex());
    LOG.info("Class for index = " + reader.indexing().getIndex().getClass().getName());  
  }
  
  Contig desiredContigForReference(SAMSequenceRecord reference) {
    Contig contig = contigsByReference.get(reference.getSequenceName());
    if (contig == null) {
      if (allReferences) {
        contig = new Contig(reference.getSequenceName(), 0, -1);
      } else {
        LOG.fine("No contig requested for reference " + reference.getSequenceName());
        return null;
      }
    }
    assert contig != null;
    if (contig.end <= 0) {
      contig = new Contig(contig.referenceName, contig.start, reference.getSequenceLength());
      LOG.info("Updated length for contig for " + contig.referenceName + " to " + contig.end);
    }
    return contig;
  }
  
  void createShardsForReference(SAMSequenceRecord reference, Contig contig) {
    final BitSet overlappingBins = GenomicIndexUtil.regionToBins(
        (int) contig.start, (int) contig.end);
    if (overlappingBins == null) {
      LOG.warning("No overlapping bins for " + contig.start + ":" + contig.end);
      return;
    }
    
    BAMShard currentShard = null;
    for (int binIndex = overlappingBins.nextSetBit(0); binIndex >= 0; binIndex = overlappingBins.nextSetBit(binIndex + 1)) {
      final Bin bin = index.getBinData(reference.getSequenceIndex(), binIndex);
      if (bin == null) {
        continue;
      }
      if (LOG.isLoggable(Level.FINE)) {
        LOG.fine("Processing bin " + index.getFirstLocusInBin(bin) + "-"
            + index.getLastLocusInBin(bin));
      }
      
      if (index.getLevelForBin(bin) != (GenomicIndexUtil.LEVEL_STARTS.length - 1)) {
        if (LOG.isLoggable(Level.FINEST)) {
          LOG.finest("Skipping - not lowest");
        }
        continue; 
        // Skip non-lowest level bins
        // Its ok to skip higher level bins
        // because in BAMShard#finalize we
        // will get all chunks overlapping a genomic region that
        // we end up having for the shard, so we will not
        // miss any reads on the boundary.
      }
      final List<Chunk> chunksInBin = bin.getChunkList();
      if (chunksInBin.isEmpty()) {
        if (LOG.isLoggable(Level.FINEST)) {
          LOG.finest("Skipping - empty");
        }
        continue; // Skip empty bins
      }

      if (currentShard == null) {
        int startLocus = 
            Math.max(index.getFirstLocusInBin(bin), (int)contig.start);
        
        if (LOG.isLoggable(Level.FINE)) {
          LOG.fine("Creating shard starting from " + startLocus);
        }
        currentShard = new BAMShard(filePath, reference.getSequenceName(),
            startLocus);
      }
      currentShard.addBin(chunksInBin, index.getLastLocusInBin(bin));
      if (LOG.isLoggable(Level.FINE)) {
        LOG.fine("Extending the shard  to " + index.getLastLocusInBin(bin));
      }

      if (shardingPolicy.shardBigEnough(currentShard)) {
        LOG.info("Shard size is big enough to finalize: " + 
            currentShard.sizeInLoci() + ", " + currentShard.approximateSizeInBytes() + " bytes");
        output.output(currentShard.finalize(index, Math.min(index.getLastLocusInBin(bin), (int)contig.end)));
        currentShard = null;
      }
    }
    if (currentShard != null) {
      LOG.info("Outputting last shard of size " + 
          currentShard.sizeInLoci() + ", " + currentShard.approximateSizeInBytes() + " bytes");
      output.output(currentShard.finalize(index, (int)contig.end));
    }
  }
}
