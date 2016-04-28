/*
 * Copyright (C) 2014 Google Inc.
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
import com.google.api.services.genomics.model.VariantCall;

import java.util.Arrays;
import java.util.List;

public class DataUtils {

  public static VariantCall makeCall(String name, Integer... alleles) {
    return new VariantCall().setCallSetName(name).setGenotype(Arrays.asList(alleles));
  }

  public static com.google.genomics.v1.VariantCall makeVariantCall(String name, Integer... alleles) {
    return com.google.genomics.v1.VariantCall.newBuilder()
        .setCallSetName(name)
        .addAllGenotype(Arrays.asList(alleles))
        .build();
  }

  public static Variant makeSimpleVariant(VariantCall... calls) {
    return new Variant().setCalls(Arrays.asList(calls));
  }

  public static Variant makeVariant(String referenceName, long start, long end,
      String referenceBases, List<String> alternateBases, VariantCall... calls) {
    Variant variant =
        new Variant().setReferenceName(referenceName).setStart(start).setEnd(end)
            .setReferenceBases(referenceBases).setAlternateBases(alternateBases);
    if (null != calls) {
      variant.setCalls(Arrays.asList(calls));
    }
    return variant;
  }

}
