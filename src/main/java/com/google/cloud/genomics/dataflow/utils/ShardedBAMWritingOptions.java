package com.google.cloud.genomics.dataflow.utils;

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;

public interface ShardedBAMWritingOptions extends GenomicsDatasetOptions, GCSOptions {
  @Description("The Google Cloud Storage path to the BAM file to get reads data from")
  @Default.String("")
  String getBAMFilePath();

  void setBAMFilePath(String filePath);
  
  @Description("Loci per writing shard")
  @Default.Long(10000)
  long getLociPerWritingShard();
  
  void setLociPerWritingShard(long lociPerShard);
}