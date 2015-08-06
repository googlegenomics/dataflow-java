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
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;

import java.util.List;

/**
 * Contains frequency for a set of alleles for a single position on a single chromosome.
 * Used in VerifyBamId.
 */
@DefaultCoder(GenericJsonCoder.class)
public class AlleleFreq extends GenericJson {
  // Strings of length 1 of one of the following bases: ['A', 'C', 'T', 'G'].
  private String refBases;
  // List of length 1 of a String of length 1 of one of the following bases: ['A', 'C', 'T', 'G'].
  private List<String> altBases;
  // Frequency for a set of alleles for the given position on the given chromosome
  // in the range [0,1].
  private double refFreq;

  public String getRefBases() {
    return refBases;
  }

  public void setRefBases(String refBases) {
    this.refBases = refBases;
  }

  public List<String> getAltBases() {
    return altBases;
  }

  public void setAltBases(List<String> altBases) {
    this.altBases = altBases;
  }

  public double getRefFreq() {
    return refFreq;
  }

  public void setRefFreq(double refFreq) {
    this.refFreq = refFreq;
  }
}
