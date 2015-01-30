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

import java.util.Comparator;
import java.util.List;

import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

public class VariantUtils {

  public static boolean isVariant(Variant variant) {
    List<String> alternateBases = variant.getAlternateBases();
    return !(null == alternateBases || alternateBases.isEmpty());
  }

  public static boolean isSnp(Variant variant) {
    return isVariant(variant) && LENGTH_IS_1.apply(variant.getReferenceBases())
        && Iterables.all(variant.getAlternateBases(), LENGTH_IS_1);
  }

  private static final Predicate<String> LENGTH_IS_1 = Predicates.compose(Predicates.equalTo(1),
      new Function<String, Integer>() {
        @Override
        public Integer apply(String string) {
          return string.length();
        }
      });

  /**
   * Comparator for sorting calls by call set name.
   */
  public static final Comparator<Call> CALL_COMPARATOR = Ordering.natural().onResultOf(
      new Function<Call, String>() {
        @Override
        public String apply(Call call) {
          return call.getCallSetName();
        }
      });
}
