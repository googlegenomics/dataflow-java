/*
 * Copyright (C) 2015 Google Inc.
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

import com.google.api.services.genomics.model.Variant;

public class VariantUtils {

  public static boolean isSnp(Variant variant) {
    if (!isVariant(variant) || 1 != variant.getReferenceBases().length()) {
      return false;
    }
    
    for (String alt : variant.getAlternateBases()) {
      if(1 != alt.length()) {
        return false;
      }
    }
    
    return true;
  }

  public static boolean isVariant(Variant variant) {
    if (null == variant.getAlternateBases() || variant.getAlternateBases().isEmpty()) {
      return false;
    }
    return true;
  }

}
