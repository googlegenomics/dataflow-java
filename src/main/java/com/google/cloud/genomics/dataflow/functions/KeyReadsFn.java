package com.google.cloud.genomics.dataflow.functions;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.Sum.SumIntegerFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.utils.ShardedBAMWritingOptions;
import com.google.cloud.genomics.utils.Contig;

public class KeyReadsFn extends DoFn<Read, KV<Contig,Read>> {
  private Aggregator<Integer, Integer> readCountAggregator;
  private Aggregator<Integer, Integer> unmappedReadCountAggregator;
  private long lociPerShard;
  
  public KeyReadsFn() {
    readCountAggregator = createAggregator("Keyed reads", new SumIntegerFn());
    unmappedReadCountAggregator = createAggregator("Keyed unmapped reads", new SumIntegerFn());
  }
  
  @Override
  public void startBundle(Context c) {
  lociPerShard = c.getPipelineOptions()
      .as(ShardedBAMWritingOptions.class)
      .getLociPerWritingShard();
  }

  @Override
  public void processElement(DoFn<Read, KV<Contig, Read>>.ProcessContext c)
    throws Exception {
    final Read read = c.element();
    c.output(
        KV.of(
            shardKeyForRead(read, lociPerShard), 
            read));
    readCountAggregator.addValue(1);
    if (isUnmapped(read)) {
      unmappedReadCountAggregator.addValue(1);
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

  static Contig shardKeyForRead(Read read, long lociPerShard) {
    String referenceName = null;
    Long alignmentStart = null;
    if (read.getAlignment() != null) {
      if (read.getAlignment().getPosition() != null ) {
        referenceName = read.getAlignment().getPosition().getReferenceName();
        alignmentStart = read.getAlignment().getPosition().getPosition();
      }
    }
    // If this read is unmapped but its mate is mapped, group them together
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