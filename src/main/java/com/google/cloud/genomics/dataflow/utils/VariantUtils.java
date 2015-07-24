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

import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import java.util.Comparator;
import java.util.List;

public class VariantUtils {

  public static final String GATK_NON_VARIANT_SEGMENT_ALT = "<NON_REF>";
 
  /**
   * Determine whether the variant has any values in alternate bases.
   */
  public static final Predicate<Variant> HAS_ALTERNATE = new Predicate<Variant>() {
    @Override
    public boolean apply(Variant variant) {
      List<String> alternateBases = variant.getAlternateBases();
      return !(null == alternateBases || alternateBases.isEmpty());
    }
  };

  public static final Predicate<String> LENGTH_IS_1 = Predicates.compose(Predicates.equalTo(1),
      new Function<String, Integer>() {
        @Override
        public Integer apply(String string) {
          return string.length();
        }
      });

  /**
   * Determine whether the variant is a SNP.
   */
  public static final Predicate<Variant> IS_SNP = Predicates.and(HAS_ALTERNATE,
      new Predicate<Variant>() {
        @Override
        public boolean apply(Variant variant) {
          return LENGTH_IS_1.apply(variant.getReferenceBases())
              && Iterables.all(variant.getAlternateBases(), LENGTH_IS_1);
        }
      });

  /**
   * Determine whether the variant is a non-variant segment (a.k.a. non-variant block record).
   * 
   * For Complete Genomics data and gVCFs such as Platinum Genomes, we wind up with zero alternates
   * (the missing value indicator "." in the VCF ALT field gets converted to null). See
   * https://sites.google.com/site/gvcftools/home/about-gvcf for more detail.
   */
  public static final Predicate<Variant> IS_NON_VARIANT_SEGMENT_WITH_MISSING_ALT = Predicates.and(
      Predicates.not(HAS_ALTERNATE), new Predicate<Variant>() {
        @Override
        public boolean apply(Variant variant) {
          // The same deletion can be specified as [CAG -> C] or [AG -> null], so double check that
          // the reference bases are also of length 1 when there are no alternates.
          return LENGTH_IS_1.apply(variant.getReferenceBases());
        }
      });

  /**
   * Determine whether the variant is a non-variant segment (a.k.a. non-variant block record).
   * 
   * For data processed by GATK the value of ALT is "&lt;NON_REF&gt;". See
   * https://www.broadinstitute.org/gatk/guide/article?id=4017 for more detail.
   */
  public static final Predicate<Variant> IS_NON_VARIANT_SEGMENT_WITH_GATK_ALT = Predicates.and(
      HAS_ALTERNATE, new Predicate<Variant>() {
        @Override
        public boolean apply(Variant variant) {
          return Iterables.all(variant.getAlternateBases(),
              Predicates.equalTo(GATK_NON_VARIANT_SEGMENT_ALT));
        }
      });

  /**
   * Determine whether the variant is a non-variant segment (a.k.a. non-variant block record).
   */
  public static final Predicate<Variant> IS_NON_VARIANT_SEGMENT = Predicates.or(
      IS_NON_VARIANT_SEGMENT_WITH_MISSING_ALT, IS_NON_VARIANT_SEGMENT_WITH_GATK_ALT);


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
