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

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.genomics.dataflow.functions.CallSimilarityCalculatorFactory;
import com.google.cloud.genomics.dataflow.functions.SharedMinorAllelesCalculatorFactory;

/**
 * An class the specifies the options for the Identity By State pipeline.
 */
public interface IdentityByStateOptions extends GenomicsDatasetOptions {

  @Validation.Required
  @Description("The class that determines the strategy for calculating the similarity of alleles.")
  @Default.Class(SharedMinorAllelesCalculatorFactory.class)
  Class<? extends CallSimilarityCalculatorFactory> getCallSimilarityCalculatorFactory();

  void setCallSimilarityCalculatorFactory(Class<? extends CallSimilarityCalculatorFactory> kls);

}
