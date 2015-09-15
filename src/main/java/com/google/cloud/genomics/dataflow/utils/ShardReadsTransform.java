package com.google.cloud.genomics.dataflow.utils;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.utils.Contig;

import com.google.cloud.genomics.dataflow.functions.KeyReadsFn;
import com.google.cloud.genomics.dataflow.utils.ShardedBAMWritingOptions;

public class ShardReadsTransform extends PTransform<PCollection<Read>, PCollection<KV<Contig, Iterable<Read>>>> {
  @Override
  public PCollection<KV<Contig, Iterable<Read>>> apply(PCollection<Read> reads) {
    return reads
      .apply(ParDo.named("KeyReads").of(new KeyReadsFn()))
      .apply(GroupByKey.<Contig, Read>create());
  }

  public static PCollection<KV<Contig, Iterable<Read>>> shard(PCollection<Read> reads) {
    return (new ShardReadsTransform()).apply(reads);
  }
}
  