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
import com.google.api.services.genomics.model.SearchVariantSetsRequest;
import com.google.api.services.genomics.model.Variant;
import com.google.api.services.genomics.model.VariantSet;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.Paginator;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import java.io.IOException;
import java.security.GeneralSecurityException;

import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Utility methods for working with genetic variant data.
 */
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
   * For data processed by GATK the value of ALT is "<NON_REF>". See
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

  /**
   * Determines whether a variant is called according to the provided filtering, using the notation
   * required by the VCF 4.2 standard. This means that the filter field of the variant must
   * contain either only "PASS" or ".".
   */
  public static final SerializableFunction<com.google.genomics.v1.Variant, Boolean> IS_PASSING
      = new SerializableFunction<com.google.genomics.v1.Variant, Boolean>() {
        @Override
        public Boolean apply(com.google.genomics.v1.Variant v) {
          return (v.getFilterCount() == 1) && (v.getFilter(0).equalsIgnoreCase("PASS")
                                                 || v.getFilter(0).equalsIgnoreCase("."));
        }
      };

  /**
   * Determines whether a Variant is from a chromosome.
   */
  public static final SerializableFunction<com.google.genomics.v1.Variant, Boolean> IS_ON_CHROMOSOME
      = new SerializableFunction<com.google.genomics.v1.Variant, Boolean>() {
        @Override
        public Boolean apply(com.google.genomics.v1.Variant v) {
          return Pattern.compile("^(chr)?(X|Y|([12]?\\d))$")
              .matcher(v.getReferenceName())
              .matches();
        }
      };

  /**
   * Determines whether a Variant's quality is the lowest level (Phred score 0).
   */
  public static final SerializableFunction<com.google.genomics.v1.Variant, Boolean>
      IS_NOT_LOW_QUALITY = new SerializableFunction<com.google.genomics.v1.Variant, Boolean>() {
        @Override
        public Boolean apply(com.google.genomics.v1.Variant v) {
          return (v.getQuality() != 0.0);
        }
      };

  /**
   * Determines whether a Variant is a SNP with a single alternative base.
   *
   * <p>To the output of @link{VariantRatios.IsSnpFn} we add the condition that
   * the variant has exactly one alternative base.
   */
  public static final SerializableFunction<com.google.genomics.v1.Variant, Boolean>
      IS_SINGLE_ALTERNATE_SNP = new SerializableFunction<com.google.genomics.v1.Variant, Boolean>(){
        @Override
        public Boolean apply(com.google.genomics.v1.Variant v) {
          return (v.getAlternateBasesList().size() == 1) && isSnp(v);
        }
      };

  /**
   * Determines whether a variant represents a single-nucleotide polymorphism (SNP).
   *
   * <p>To be a SNP, a variant must:
   * <ul>
   * <li> Correspond to exactly 1 position on the reference genome.</li>
   * <li> Have at least one alternative base.</li>
   * <li> Each alternative base must correspond to exactly 1 position.</li>
   * </ul>
   *
   * @param variant the variant for which to check the mutation class
   */
  public static final boolean isSnp(com.google.genomics.v1.Variant variant) {
    // SNPs must be linked to a single nucleotide in the reference genome.
    if (variant.getReferenceBases().length() != 1) {
      return false;
    }
    // We require at least one alternative.
    if (variant.getAlternateBasesList().size() < 1) {
      return false;
    }
    // We also require that each alternative is exactly one nucleotide.
    // This excludes more complex insertions.
    for (String alt : variant.getAlternateBasesList()) {
      if (alt.length() != 1) {
        return false;
      }
    }
    // Fallthrough: it's a SNP.
    return true;
  }
}
