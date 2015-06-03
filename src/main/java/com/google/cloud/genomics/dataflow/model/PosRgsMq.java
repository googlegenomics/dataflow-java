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
import com.google.api.client.util.Key;
import com.google.api.client.util.Value;
import com.google.api.services.genomics.model.Position;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import java.util.Objects;

/**
 * Wrapper class for Position, ReadGroupSetId and mapping quality objects for use in the
 * CalculateCoverage pipeline
 */
@DefaultCoder(GenericJsonCoder.class)
public class PosRgsMq extends GenericJson {

  @Key
  private Position pos;
  @Key
  private String rgsId;
  @Key
  private MappingQuality mq;

  /**
   * MappingQuality enum that contains the 4 possible MappingQuality types (Low(L), Medium(M),
   * High(H), and All(A)).
   */
  @DefaultCoder(AvroCoder.class)
  public enum MappingQuality {@Value L, @Value M, @Value H, @Value A}

  public PosRgsMq() {
    this.pos = new Position().setPosition(0L).setReferenceName("");
    this.rgsId = "";
    this.mq = MappingQuality.A;
  }

  public PosRgsMq(Position pos, String rgsId, MappingQuality mq) {
    this.pos = pos;
    this.rgsId = rgsId;
    this.mq = mq;
  }

  public Position getPos() {
    return pos;
  }

  public void setPos(Position pos) {
    this.pos = pos;
  }

  public String getRgsId() {
    return rgsId;
  }

  public void setRgsId(String rgsId) {
    this.rgsId = rgsId;
  }

  public MappingQuality getMq() {
    return mq;
  }

  public void setMq(MappingQuality mq) {
    this.mq = mq;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof PosRgsMq)) {
      return false;
    }
    PosRgsMq test = (PosRgsMq) o;
    return test.pos.equals(this.pos) && test.rgsId.equals(this.rgsId) && test.mq.equals(this.mq);
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 19 * hash + Objects.hashCode(this.pos);
    hash = 19 * hash + Objects.hashCode(this.rgsId);
    hash = 19 * hash + Objects.hashCode(this.mq);
    return hash;
  }

  @Override
  public PosRgsMq set(String fieldName, Object value) {
    return (PosRgsMq) super.set(fieldName, value);
  }

  @Override
  public PosRgsMq clone() {
    return (PosRgsMq) super.clone();
  }
}
