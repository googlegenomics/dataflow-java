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

import java.util.Objects;

/**
 * Data on a single base in a read, used for pileup.
 */
@DefaultCoder(GenericJsonCoder.class)
public class ReadBaseQuality extends GenericJson {

  private String base;
  private int quality;

  public ReadBaseQuality() {
    this.base = "";
  }

  public ReadBaseQuality(String base, int quality) {
    this.base = base;
    this.quality = quality;
  }

  public String getBase() {
    return base;
  }

  public void setBase(String base) {
    this.base = base;
  }

  public int getQuality() {
    return quality;
  }

  public void setQuality(int quality) {
    this.quality = quality;
  }

  @Override
  public boolean equals(Object o) {
        if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof ReadBaseQuality)) {
      return false;
    }
    ReadBaseQuality test = (ReadBaseQuality) o;
    return test.base.equals(this.base) && test.quality == this.quality;
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 37 * hash + Objects.hashCode(this.base);
    hash = 37 * hash + this.quality;
    return hash;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "ReadBaseQuality [base=" + base + ", quality=" + quality + "]";
  }
}
