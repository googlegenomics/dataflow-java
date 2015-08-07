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

/**
 * Different sharding policies and constants governing
 * how we slice the data in BAM file.
 */
public interface ShardingPolicy  {
  /**
   * Decides whether a shard we are growing is large enough to be finalized
   * and submitted for processing.
   */
  public boolean shardBigEnough(BAMShard shard);
  
  static final int MAX_BYTES_PER_SHARD = 10*1024*1024;    // 10MB
  public static ShardingPolicy BYTE_SIZE_POLICY =
   new ShardingPolicy() {
      @Override
      public boolean shardBigEnough(BAMShard shard) {
        return shard.approximateSizeInBytes() > MAX_BYTES_PER_SHARD;
      }
    };
  
  static final int MAX_BASE_PAIRS_PER_SHARD = 100000;
  public static ShardingPolicy LOCI_SIZE_POLICY = 
    new ShardingPolicy() {
      @Override
      public boolean shardBigEnough(BAMShard shard) {
        return shard.sizeInLoci() > MAX_BASE_PAIRS_PER_SHARD;
      }
    };
}

