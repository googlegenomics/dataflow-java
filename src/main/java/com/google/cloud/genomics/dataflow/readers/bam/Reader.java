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
  ValidationStringency stringency;
  
  SAMRecordIterator iterator;
  boolean procesingUnmapped;
  
  int recordsBeforeStart = 0;
  int recordsAfterEnd = 0;
  int mismatchedSequence = 0;
  int recordsProcessed = 0;
  
  public Reader(Objects storageClient, ValidationStringency stringency, BAMShard shard, DoFn<BAMShard, Read>.ProcessContext c) {
    super();
    this.storageClient = storageClient;
    this.shard = shard;
    this.c = c;
    this.stringency = stringency;
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
    final SamReader reader = BAMIO.openBAM(storageClient, shard. file, stringency);
    iterator = null;
    procesingUnmapped = shard.contig.referenceName.equals("*");
    if (reader.hasIndex() && reader.indexing() != null) {
      if (procesingUnmapped) {
        LOG.info("Processing unmapped");
        iterator = reader.queryUnmapped();
      } else if (shard.span != null) {
        LOG.info("Processing span");
        iterator = reader.indexing().iterator(shard.span);
      } else if (shard.contig.referenceName != null && !shard.contig.referenceName.isEmpty()) {
        LOG.info("Processing all bases for " + shard.contig.referenceName);
        iterator = reader.query(shard.contig.referenceName, (int) shard.contig.start,
            (int) shard.contig.end, false);
      } 
    }
    if (iterator == null) {
      LOG.info("Processing all reads");
      iterator = reader.iterator();
    }
  }

  boolean isWrongSequence(SAMRecord record) {
    return (procesingUnmapped && !record.getReadUnmappedFlag()) ||
        (!procesingUnmapped && (record.getReadUnmappedFlag() || 
            (shard.contig.referenceName != null && 
            !shard.contig.referenceName.isEmpty() &&
            !shard.contig.referenceName.equals(record.getReferenceName()))));
  }
  
  void processRecord(SAMRecord record) {
    if (isWrongSequence(record)) {
      mismatchedSequence++;
      return;
    }
    if (record.getAlignmentStart() < shard.contig.start) {
      recordsBeforeStart++;
      return;
    }
    if (record.getAlignmentStart() >= shard.contig.end) {
      recordsAfterEnd++;
      return;
    }
    c.output(ReadConverter.makeRead(record));
    recordsProcessed++;
  }
  
  void dumpStats() {
    timer.stop();
    LOG.info("Processed " + recordsProcessed + 
        " in " + timer + 
        ". Speed: " + (recordsProcessed*1000)/timer.elapsed(TimeUnit.MILLISECONDS) + " reads/sec"
        + ", skipped other sequences " + mismatchedSequence 
        + ", skippedBefore " + recordsBeforeStart
        + ", skipped after " + recordsAfterEnd);
  }
  
  /**
   * To compare how sharded reading works vs. plain HTSJDK sequential iteration,
   * this method implements such iteration.
   * This makes it easier to discover errors such as reads that are somehow
   * skipped by a sharded approach.
   */
  public static Iterable<Read> readSequentiallyForTesting(Objects storageClient, String storagePath, Contig contig, ValidationStringency stringency) throws IOException {
    Stopwatch timer = Stopwatch.createStarted();
    SamReader samReader = BAMIO.openBAM(storageClient, storagePath, stringency);
    SAMRecordIterator iterator =  samReader.queryOverlapping(contig.referenceName, 
        (int) contig.start + 1,
        (int) contig.end + 1);
    List<Read> reads = new ArrayList<Read>(); 
    
    int recordsBeforeStart = 0;
    int recordsAfterEnd = 0;
    int mismatchedSequence = 0;
    int recordsProcessed = 0;
    while (iterator.hasNext()) {
      SAMRecord record = iterator.next();
      final boolean wrongSequence = !contig.referenceName.equals(record.getReferenceName())
          || (!contig.referenceName.equals("*") && record.getReadUnmappedFlag());
      
      if (wrongSequence) {
        mismatchedSequence++;
        continue;
      }
      if (record.getAlignmentStart() < contig.start) {
        recordsBeforeStart++;
        continue;
      }
      if (record.getAlignmentStart() >= contig.end) {
        recordsAfterEnd++;
        continue;
      }
      reads.add(ReadConverter.makeRead(record)); 
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
