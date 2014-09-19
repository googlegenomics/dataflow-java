/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dataflow.functions;

import com.google.api.services.genomics.model.Call;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Emits a (Variant name, boolean) pair, for each family-variant pair.
 * where
 * true indicates both 1st parent and child has the variant, and
 * false indicates that both 2nd parent and the child has the variant.
 */
public class ExtractFamilyVariantStatus
  extends DoFn<Variant, KV<String, Boolean>> {

  private class Trio {
    String mom, dad, child;
    Trio(String m, String d, String c) {
      mom = m;
      dad = d;
      child = c;
    }
  }

  List<Trio> getTrios() {
    // TODO(gunan): Replace this with actual fetching of family informtion.
    List<Trio> trioList = Lists.newArrayList();
    // All Trio's are ordered as "mom, dad and child".
    trioList.add(new Trio("NA12889", "NA12890", "NA12877"));
    trioList.add(new Trio("NA12891", "NA12892", "NA12878"));
    trioList.add(new Trio("NA12877", "NA12878", "NA12879"));
    trioList.add(new Trio("NA12877", "NA12878", "NA12880"));
    trioList.add(new Trio("NA12877", "NA12878", "NA12881"));
    trioList.add(new Trio("NA12877", "NA12878", "NA12882"));
    trioList.add(new Trio("NA12877", "NA12878", "NA12883"));
    trioList.add(new Trio("NA12877", "NA12878", "NA12884"));
    trioList.add(new Trio("NA12877", "NA12878", "NA12885"));
    trioList.add(new Trio("NA12877", "NA12878", "NA12886"));
    trioList.add(new Trio("NA12877", "NA12878", "NA12887"));
    trioList.add(new Trio("NA12877", "NA12878", "NA12888"));
    trioList.add(new Trio("NA12877", "NA12878", "NA12893"));
    return trioList;
  }

  @Override
  public void processElement(ProcessContext c) {
    Variant variant = c.element();
    List<String> samples = getSamplesWithVariant(variant);
    List<Trio> trioList = getTrios();

    for (Trio family : trioList) {
      if (samples.contains(family.mom) &&
          (!samples.contains(family.dad)) &&
          samples.contains(family.child)) {
        c.output(KV.of(variant.getId(), true));
      } else if ((!samples.contains(family.mom)) &&
                 samples.contains(family.dad) &&
                 samples.contains(family.child)) {
        c.output(KV.of(variant.getId(), false));
      }
    }
  }

  @VisibleForTesting
  List<String> getSamplesWithVariant(Variant variant) {
    List<String> samplesWithVariant = Lists.newArrayList();
    for (Call call : variant.getCalls()) {
      samplesWithVariant.add(call.getCallsetName());
    }
    return samplesWithVariant;
  }
}
