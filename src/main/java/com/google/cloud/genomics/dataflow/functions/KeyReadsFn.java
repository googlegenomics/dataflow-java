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
package com.google.cloud.genomics.dataflow.functions;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import com.google.cloud.genomics.utils.Contig;
import com.google.genomics.v1.Read;

import java.util.logging.Logger;

/*
 * Takes a read and associates it with a Contig.
 * This can be used to shard Reads so they can be written to disk in parallel.
 * The size of the Contigs is determined by Options.getLociPerWritingShard.
 */
public class KeyReadsFn extends DoFn<Read, KV<Contig,Read>> {
  private static final Logger LOG = Logger.getLogger(KeyReadsFn.class.getName());

  public static interface Options extends PipelineOptions {
    @Description("Loci per writing shard")
    @Default.Long(10000)
    long getLociPerWritingShard();

    void setLociPerWritingShard(long lociPerShard);
  }

  private long lociPerShard;
  private long count;
  private long minPos = Long.MAX_VALUE;
  private long maxPos = Long.MIN_VALUE;

  @StartBundle
  public void startBundle(StartBundleContext c) {
    lociPerShard = c.getPipelineOptions()
      .as(Options.class)
      .getLociPerWritingShard();
    count = 0;
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) {
    LOG.info("KeyReadsDone: Processed " + count + " reads" + "min=" + minPos +
        " max=" + maxPos);
  }

  @ProcessElement
  public void processElement(DoFn<Read, KV<Contig, Read>>.ProcessContext c)
    throws Exception {
    final Read read = c.element();
    long pos = read.getAlignment().getPosition().getPosition();
    minPos = Math.min(minPos, pos);
    maxPos = Math.max(maxPos, pos);
    count++;
    c.output(
        KV.of(
            shardKeyForRead(read, lociPerShard),
            read));
    Metrics.counter(KeyReadsFn.class, "Keyed reads").inc();
    if (isUnmapped(read)) {
      Metrics.counter(KeyReadsFn.class, "Keyed unmapped reads").inc();
    }
  }

  static boolean isUnmapped(Read read) {
    if (read.getAlignment() == null || read.getAlignment().getPosition() == null) {
      return true;
    }
    final String reference = read.getAlignment().getPosition().getReferenceName();
    if (reference == null || reference.isEmpty() || reference.equals("*")) {
      return true;
    }
    return false;
  }

  public static Contig shardKeyForRead(Read read, long lociPerShard) {
    String referenceName = null;
    Long alignmentStart = null;
    if (read.getAlignment() != null) {
      if (read.getAlignment().getPosition() != null ) {
        referenceName = read.getAlignment().getPosition().getReferenceName();
        alignmentStart = read.getAlignment().getPosition().getPosition();
      }
    }
    // If this read is unmapped but its mate is mapped, group them together.
    if (referenceName == null || referenceName.isEmpty() ||
        referenceName.equals("*") || alignmentStart == null) {
      if (read.getNextMatePosition() != null) {
        referenceName = read.getNextMatePosition().getReferenceName();
        alignmentStart = read.getNextMatePosition().getPosition();
      }
    }
    if (referenceName == null || referenceName.isEmpty()) {
      referenceName = "*";
    }
    if (alignmentStart == null) {
      alignmentStart = new Long(0);
    }
    return shardFromAlignmentStart(referenceName, alignmentStart, lociPerShard);
  }

  static Contig shardFromAlignmentStart(String referenceName, long alignmentStart, long lociPerShard) {
    final long shardStart = (alignmentStart / lociPerShard) * lociPerShard;
    return new Contig(referenceName, shardStart, shardStart + lociPerShard);
  }
}
