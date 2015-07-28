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

import com.google.api.services.genomics.model.Position;
import java.util.Objects;

/**
 * Data on a single base in a read connected with it reference data.
 */
public class ReadBaseWithReference {

  private ReadBaseQuality rbq;
  private String refBase;
  private Position refPosition;

  public ReadBaseWithReference() {
    this.rbq = new ReadBaseQuality();
    this.refBase = "";
    this.refPosition = new Position().setPosition(0L).setReferenceName("");
  }

  public ReadBaseWithReference(ReadBaseQuality rbq, String refBase, Position refPosition) {
    this.rbq = rbq;
    this.refBase = refBase;
    this.refPosition = refPosition;
  }

  public String getRefBase() {
    return refBase;
  }

  public void setRefBase(String refBase) {
    this.refBase = refBase;
  }

  public Position getRefPosition() {
    return refPosition;
  }

  public void setRefPosition(Position refPosition) {
    this.refPosition = refPosition;
  }

  public ReadBaseQuality getRbq() {
    return rbq;
  }

  public void setRbq(ReadBaseQuality rbq) {
    this.rbq = rbq;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 89 * hash + Objects.hashCode(this.rbq);
    hash = 89 * hash + Objects.hashCode(this.refBase);
    hash = 89 * hash + Objects.hashCode(this.refPosition);
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other == this) {
      return true;
    }
    if (!(other instanceof ReadBaseWithReference)) {
      return false;
    }
    ReadBaseWithReference test = (ReadBaseWithReference) other;
    return test.rbq.equals(this.rbq) && test.refBase.equals(this.refBase)
        && test.refPosition.equals(this.refPosition);
  }

}
