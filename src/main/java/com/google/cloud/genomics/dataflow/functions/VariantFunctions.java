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
package com.google.cloud.genomics.dataflow.functions;

import java.util.regex.Pattern;

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.genomics.utils.grpc.VariantUtils;
import com.google.genomics.v1.Variant;

/**
 * Utility methods for working with genetic variant data.
 */
public class VariantFunctions {

  /**
   * Determines whether a variant is called according to the provided filtering, using the notation
   * required by the VCF 4.2 standard. This means that the filter field of the variant must
   * contain either only "PASS" or ".".
   */
  public static final SerializableFunction<Variant, Boolean> IS_PASSING
      = new SerializableFunction<Variant, Boolean>() {
        @Override
        public Boolean apply(Variant v) {
          return (v.getFilterCount() == 1) && (v.getFilter(0).equalsIgnoreCase("PASS")
                                                 || v.getFilter(0).equalsIgnoreCase("."));
        }
      };

  /**
   * Determines whether a Variant is from a chromosome.
   */
  public static final SerializableFunction<Variant, Boolean> IS_ON_CHROMOSOME
      = new SerializableFunction<Variant, Boolean>() {
        @Override
        public Boolean apply(Variant v) {
          return Pattern.compile("^(chr)?(X|Y|([12]?\\d))$")
              .matcher(v.getReferenceName())
              .matches();
        }
      };

  /**
   * Determines whether a Variant's quality is the lowest level (Phred score 0).
   */
  public static final SerializableFunction<Variant, Boolean>
      IS_NOT_LOW_QUALITY = new SerializableFunction<Variant, Boolean>() {
        @Override
        public Boolean apply(Variant v) {
          return (v.getQuality() != 0.0);
        }
      };

  /**
   * Determines whether a Variant is a SNP with a single alternative base.
   *
   * <p>To the output of @link{VariantRatios.IsSnpFn} we add the condition that
   * the variant has exactly one alternative base.
   */
  public static final SerializableFunction<Variant, Boolean>
      IS_SINGLE_ALTERNATE_SNP = new SerializableFunction<Variant, Boolean>(){
        @Override
        public Boolean apply(Variant v) {
          return (v.getAlternateBasesList().size() == 1) && VariantUtils.IS_SNP.apply(v);
        }
      };

}
