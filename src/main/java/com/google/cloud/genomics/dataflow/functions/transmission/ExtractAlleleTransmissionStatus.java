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
package com.google.cloud.genomics.dataflow.functions.transmission;

import com.google.api.services.genomics.model.Variant;
import com.google.api.services.genomics.model.VariantCall;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.model.Allele;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Emits String, (Allele, Boolean) pairs for each Allele a parent has.
 * The String is the Id of the Variant, and the Boolean is set to True
 * if the child has the Allele, False if not.
 * i.e.
 *  If the genotype of the family is:
 *    Mother: AC, Father TG, Child is AG, function emits:
 *      (A, True), (C, False), (T, False), (G, True)
 */
public class ExtractAlleleTransmissionStatus
    extends DoFn<Variant, KV<Allele, Boolean>> {

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
    List<Trio> trioList = getTrios();

    List<String> sequences = variant.getAlternateBases();
    sequences.add(0, variant.getReferenceBases());

    for (Trio family : trioList) {
      List<Integer> childGenotypes = getSample(variant, family.child)
          .getGenotype();

      List<Integer> parentGenotypes = Lists.newArrayList();
      parentGenotypes.addAll(getSample(variant, family.mom).getGenotype());
      parentGenotypes.addAll(getSample(variant, family.dad).getGenotype());

      for (Integer i: parentGenotypes) {
        c.output(KV.of(new Allele(variant.getId(),
                                  variant.getStart(),
                                  sequences.get(i)),
                       childGenotypes.contains(i)));
        childGenotypes.remove(i);
      }
    }
  }

  @VisibleForTesting
  VariantCall getSample(Variant variant, String sampleName) {
    for (VariantCall call : variant.getCalls()) {
      if (call.getCallSetName() == sampleName)
        return call;
    }
    return null;
  }
}
