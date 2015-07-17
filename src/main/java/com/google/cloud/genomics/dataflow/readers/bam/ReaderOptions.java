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

import htsjdk.samtools.ValidationStringency;

import java.io.Serializable;

/**
 * A collection of options governing how BAM files are read.
 */
public class ReaderOptions implements Serializable{
  ValidationStringency stringency = ValidationStringency.DEFAULT_STRINGENCY;
  
  /**
   * If true, we will read and return unmapped reads in the order expected by
   * HTSDK/GATK/Picard tools, specifically unmapped mate pairs of mapped reads
   * will be returned according to their position specified in the BAM record
   * (as opposed to being after all the mapped reads). This position is 
   * typically specified such that these unmapped mates immediately follow 
   * their mapped counterparts. 
   */
  boolean includeUnmappedReads = false;
  
  public ReaderOptions() {
    
  }
  
  public ReaderOptions(ValidationStringency stringency, boolean includeUnmappedReads) {
    this.stringency = stringency;
    this.includeUnmappedReads = includeUnmappedReads;
  }

  public ValidationStringency getStringency() {
    return stringency;
  }
  
  public void setStringency(ValidationStringency stringency) {
    this.stringency = stringency;
  }
  
  public boolean getIncludeUnmappedReads() {
    return includeUnmappedReads;
  }
  
  public void setIncludeUnmappedReads(boolean includeUnmappedReads) {
    this.includeUnmappedReads = includeUnmappedReads;
  }
}

