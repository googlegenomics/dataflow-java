/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dataflow.utils;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/**
 * A common options class for pipelines that shard by genomic region.
 */
public interface ShardOptions extends GenomicsOptions {

  @Description("By default, variants analyses will be run on BRCA1.  Pass this flag to run on all "
      + "references present in the dataset.  Note that certain jobs such as PCA and IBS "
      + "will automatically exclude X and Y chromosomes when this option is true.")
  @Default.Boolean(false)
  boolean isAllReferences();
  void setAllReferences(boolean allReferences);

  @Description("Comma separated tuples of reference:start:end,... ")
  @Default.String("17:41196311:41277499")
  String getReferences();
  void setReferences(String references);

  @Description("The maximum number of bases per shard.")
  @Default.Long(1000000)
  long getBasesPerShard();
  void setBasesPerShard(long basesPerShard);

}
