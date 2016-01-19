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

/**
 * This class is used to count the number of reads aligned to a SNP that show the reference base,
 * the non-reference base, some other base, or an unknown base.  Within each category, we count the
 * number with each quality score.
 * 
 * For example, we might have 2 reads that show the reference base with quality 10, 5 reads that
 * show the non-reference base with quality 60, and 1 read that shows a different nucleotide with
 * quality 0.
 */
public class ReadQualityCount {

  private Base base;
  private int quality;
  private long count;

  /**
   * Which type of Base this ReadQualityCount represents.
   */
  public enum Base {
    UNKNOWN, REF, NONREF, OTHER
  };

  public Base getBase() {
    return base;
  }

  public void setBase(Base base) {
    this.base = base;
  }

  public int getQuality() {
    return quality;
  }

  public void setQuality(int quality) {
    this.quality = quality;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "ReadQualityCount [base=" + base + ", quality=" + quality + ", count=" + count + "]";
  }
}
