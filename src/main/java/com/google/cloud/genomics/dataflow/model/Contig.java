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

import static com.google.common.base.Objects.equal;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

/**
 * Represents a segment of a genome.
 */
public class Contig {
  private String referenceName;
  private long start;
  private long end;

  public Contig(String referenceName, long start, long end) {
    this.referenceName = requireNonNull(referenceName);
    this.start = start;
    this.end = end;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  @Override
  public int hashCode() {
    return hash(referenceName, start, end);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Contig)) {
      return false;
    }
    Contig c = (Contig) obj;
    return equal(referenceName, c.referenceName) && equal(start, c.start) && equal(end, c.end);
  }
  
}
