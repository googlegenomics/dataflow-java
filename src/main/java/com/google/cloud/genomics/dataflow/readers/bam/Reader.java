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

import com.google.api.services.genomics.model.Read;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.ReadUtils;
import com.google.common.base.Stopwatch;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.ValidationStringency;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Get all the reads specified by a given shard from a given BAM file.
 * Meant to be called from the DoFn processing the shard.
 */
public class Reader {
  private static final Logger LOG = Logger.getLogger(Reader.class.getName());
  
  Storage.Objects storageClient;
  BAMShard shard;
  DoFn<BAMShard, Read>.ProcessContext c;
  Stopwatch timer;
  ReaderOptions options;
  
  SAMRecordIterator iterator;
  enum Filter {
    UNMAPPED_ONLY,
    MAPPED_AND_UNMAPPED,
    MAPPED_ONLY
  }
  
  Filter filter;
  
  public int recordsBeforeStart = 0;
  public int recordsAfterEnd = 0;
  public int mismatchedSequence = 0;
  public int recordsProcessed = 0;
  public int readsGenerated = 0;
  
  public Reader(Objects storageClient, ReaderOptions options, BAMShard shard, DoFn<BAMShard, Read>.ProcessContext c) {
    super();
    this.storageClient = storageClient;
    this.shard = shard;
    this.c = c;
    this.options = options;
    filter = setupFilter(options, shard.contig.referenceName);
  }
  
  public static Filter setupFilter(ReaderOptions options, String referenceName) {
    /* 
     * The common way of asking for unmapped reads is by specifying asterisk
     * as the sequence name.
     * From SAM format, section 1.4. paragraph 3:
     * "RNAME: Reference sequence NAME of the alignment. 
     * If @SQ header lines are present, RNAME (if not ‘*’) must be present in 
     * one of the SQ-SN tag. 
     * An unmapped segment without coordinate has a ‘*’ at this field."
     * https://samtools.github.io/hts-specs/SAMv1.pdf
     */
    if (referenceName.equals("*")) {
      return Filter.UNMAPPED_ONLY;
    } else if (options.getIncludeUnmappedReads()) {
      return Filter.MAPPED_AND_UNMAPPED;
    }
    return Filter.MAPPED_ONLY;
  }
  
  public void process() throws IOException {
    timer = Stopwatch.createStarted();
    openFile();
    
    while (iterator.hasNext()) {
      processRecord(iterator.next());
    }
    
    dumpStats();
  }

  void openFile() throws IOException {
    LOG.info("Processing shard " + shard);
    final SamReader reader = BAMIO.openBAM(storageClient, shard.file, 
        options.getStringency());
    iterator = null;
    if (reader.hasIndex() && reader.indexing() != null) {
      if (filter == Filter.UNMAPPED_ONLY) {
        LOG.info("Processing unmapped");
        iterator = reader.queryUnmapped();
      } else if (shard.span != null) {
        LOG.info("Processing span for " + shard.contig);
        iterator = reader.indexing().iterator(shard.span);
      } else if (shard.contig.referenceName != null && !shard.contig.referenceName.isEmpty()) {
        LOG.info("Processing all bases for " + shard.contig);
        iterator = reader.query(shard.contig.referenceName, (int) shard.contig.start,
            (int) shard.contig.end, false);
      } 
    }
    if (iterator == null) {
      LOG.info("Processing all reads");
      iterator = reader.iterator();
    }
  }

  /**
   * Checks if the record matches our filter.
   */
  static boolean passesFilter(SAMRecord record, Filter filter, 
      String referenceName) {
    // If we are looking for only mapped or only unmapped reads then we will use
    // the UnmappedFlag to decide if this read should be rejected.
    if (filter == Filter.UNMAPPED_ONLY && !record.getReadUnmappedFlag()) {
      return false;
    }
    
    if (filter == Filter.MAPPED_ONLY && record.getReadUnmappedFlag()) {
      return false;
    }
    
    // If we are looking for mapped reads, then we check the reference name
    // of the read matches the one we are looking for.
    final boolean referenceNameMismatch = referenceName != null && 
        !referenceName.isEmpty() &&
        !referenceName.equals(record.getReferenceName());
    
    // Note that unmapped mate pair of mapped read will have a reference
    // name set to the reference of its mapped mate.
    if ((filter == Filter.MAPPED_ONLY || filter == Filter.MAPPED_AND_UNMAPPED)
        && referenceNameMismatch) {
      return false;
    }
    
    return true;
  }
  
  boolean passesFilter(SAMRecord record) {
    return passesFilter(record, filter, shard.contig.referenceName);
  }
  
  void processRecord(SAMRecord record) {
    recordsProcessed++;
    if (!passesFilter(record)) {
      mismatchedSequence++;
      return;
    }
    if (record.getAlignmentStart() < shard.contig.start) {
      recordsBeforeStart++;
      return;
    }
    if (record.getAlignmentStart() > shard.contig.end) {
      recordsAfterEnd++;
      return;
    }
    c.output(ReadUtils.makeRead(record));
    readsGenerated++;
  }
  
  void dumpStats() {
    timer.stop();
    long elapsed = timer.elapsed(TimeUnit.MILLISECONDS);
    if (elapsed == 0) elapsed = 1;
    LOG.info("Processed " + recordsProcessed + " outputted " + readsGenerated +
        " in " + timer + 
        ". Speed: " + (recordsProcessed*1000)/elapsed + " reads/sec"
        + ", filtered out by reference and mapping " + mismatchedSequence 
        + ", skippedBefore " + recordsBeforeStart
        + ", skipped after " + recordsAfterEnd);
  }
  
  /**
   * To compare how sharded reading works vs. plain HTSJDK sequential iteration,
   * this method implements such iteration.
   * This makes it easier to discover errors such as reads that are somehow
   * skipped by a sharded approach.
   */
  public static Iterable<Read> readSequentiallyForTesting(Objects storageClient,
      String storagePath, Contig contig, 
      ReaderOptions options) throws IOException {
    Stopwatch timer = Stopwatch.createStarted();
    SamReader samReader = BAMIO.openBAM(storageClient, storagePath, options.getStringency());
    SAMRecordIterator iterator =  samReader.queryOverlapping(contig.referenceName, 
        (int) contig.start + 1,
        (int) contig.end);
    List<Read> reads = new ArrayList<Read>(); 
    
    int recordsBeforeStart = 0;
    int recordsAfterEnd = 0;
    int mismatchedSequence = 0;
    int recordsProcessed = 0;
    Filter filter = setupFilter(options, contig.referenceName);
    while (iterator.hasNext()) {
      SAMRecord record = iterator.next();
      final boolean passesFilter = passesFilter(record, filter, contig.referenceName);
      
      if (!passesFilter) {
        mismatchedSequence++;
        continue;
      }
      if (record.getAlignmentStart() < contig.start) {
        recordsBeforeStart++;
        continue;
      }
      if (record.getAlignmentStart() > contig.end) {
        recordsAfterEnd++;
        continue;
      }
      reads.add(ReadUtils.makeRead(record));
      recordsProcessed++;
    }
    timer.stop();
    LOG.info("NON SHARDED: Processed " + recordsProcessed + 
        " in " + timer + 
        ". Speed: " + (recordsProcessed*1000)/timer.elapsed(TimeUnit.MILLISECONDS) + " reads/sec"
        + ", skipped other sequences " + mismatchedSequence 
        + ", skippedBefore " + recordsBeforeStart
        + ", skipped after " + recordsAfterEnd);
    return reads;
  }
}
