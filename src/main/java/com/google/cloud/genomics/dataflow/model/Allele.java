/*
 * Copyright (C) 2014 Google Inc.
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
package com.google.cloud.genomics.dataflow.model;

/**
 * A small container to store the alleles at a location
 *
 */
public class Allele {
  String referenceName;
  long position;
  String allele;

  public Allele(String referenceName, long position, String allele) {
    this.referenceName = referenceName;
    this.position = position;
    this.allele = allele;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getAllele() {
    return allele;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "Allele [referenceName=" + referenceName + ", position=" + position + ", allele="
        + allele + "]";
  }
}
