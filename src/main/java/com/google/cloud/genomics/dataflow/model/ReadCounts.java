/*
 * Copyright 2015 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.genomics.dataflow.model;

import com.google.api.client.json.GenericJson;
import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.model.ReadQualityCount.Base;

import java.util.List;

/**
 * Counts of reads for a single SNP with a single alternate value for use in the
 * VerifyBamId pipeline.  For each SNP, we accumulate counts of bases and quality scores
 * for associated aligned reads.
 */
@DefaultCoder(GenericJsonCoder.class)
public class ReadCounts extends GenericJson {
  /**
   * The count for a single base and quality score is stored in a ReadQualityCount object.
   */
  private List<ReadQualityCount> readQualityCounts = Lists.newArrayList();
  /**
   * refFreq contains the population frequency of the reference allele.
   */
  private double refFreq;

  public List<ReadQualityCount> getReadQualityCounts() {
    return readQualityCounts;
  }

  public void setReadQualityCounts(List<ReadQualityCount> readQualityCounts) {
    this.readQualityCounts = readQualityCounts;
  }

  public void addReadQualityCount(Base base, int quality, long count) {
    ReadQualityCount rqc = new ReadQualityCount();
    rqc.setBase(base);
    rqc.setCount(count);
    rqc.setQuality(quality);
    this.readQualityCounts.add(rqc);
  }

  public void addReadQualityCount(ReadQualityCount rqc) {
    this.readQualityCounts.add(rqc);
  }

  public double getRefFreq() {
    return refFreq;
  }

  public void setRefFreq(double refFreq) {
    this.refFreq = refFreq;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "ReadCounts [readQualityCounts=" + readQualityCounts + ", refFreq=" + refFreq + "]";
  }
}
