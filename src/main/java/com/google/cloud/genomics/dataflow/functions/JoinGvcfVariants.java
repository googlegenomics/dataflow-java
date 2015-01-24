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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.api.client.util.Lists;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.VariantUtils;

/**
 * The transforms in this class convert data in Genome VCF (gVCF) format to variant-only VCF data
 * with calls from block-records merged into the variants with which they overlap.  
 * 
 * This is currently done only for SNP variants.  Indels and structural variants are left as-is.
 */
public class JoinGvcfVariants {

  @SuppressWarnings("serial")
  public static class BinVariants extends DoFn<Variant, KV<KV<String, Integer>, Variant>> {

    public int getStartBin(int binSize, Variant variant) {
      // Round down to the nearest integer
      return (int) (variant.getStart() / binSize);
    }

    public int getEndBin(int binSize, Variant variant) {
      // Round down to the nearest integer
      return (int) (variant.getEnd() / binSize);
    }

    @Override
    public void processElement(ProcessContext context) {
      GenomicsDatasetOptions options =
          context.getPipelineOptions().as(GenomicsDatasetOptions.class);
      Variant variant = context.element();
      int startBin = getStartBin(options.getBinSize(), variant);
      int endBin =
          VariantUtils.isVariant(variant) ? startBin : getEndBin(options.getBinSize(), variant);
      for (int bin = startBin; bin <= endBin; bin++) {
        context.output(KV.of(KV.of(variant.getReferenceName(), bin), variant));
      }
    }
  }

  @SuppressWarnings("serial")
  public static class MergeVariants extends
      DoFn<KV<KV<String, Integer>, Iterable<Variant>>, Variant> {

    @Override
    public void processElement(ProcessContext context) {

      // The upper bound on number of variants in the iterable is dependent upon the binSize
      // used in the prior step to construct the key.
      KV<KV<String, Integer>, Iterable<Variant>> kv = context.element();

      // Sort by start position descending and ensure that if a variant
      // and a ref-matching block are at the same position, the
      // ref-matching block comes first.
      List<Variant> records = Lists.newArrayList(kv.getValue());
      Collections.sort(records, new JoinGvcfVariants.VariantComparator());

      // The upper bound on potential overlaps is the sample size plus the number of
      // block records that occur between actual variants.
      List<Variant> blockRecords = new ArrayList<Variant>();

      for (Variant record : records) {
        if (VariantUtils.isVariant(record)) {
          // TODO: determine and implement the correct criteria for overlaps of non-SNP variants
          if (VariantUtils.isSnp(record)) {
            for (Iterator<Variant> iterator = blockRecords.iterator(); iterator.hasNext();) {
              Variant blockRecord = iterator.next();
              if (JoinGvcfVariants.isOverlapping(blockRecord, record)) {
                record.getCalls().addAll(blockRecord.getCalls());
              } else {
                // Remove the current element from the iterator and the list since it is
                // left of the genomic region we are currently working on due to our sort..
                iterator.remove();
              }
            }
          }
          context.output(record);
        } else {
          blockRecords.add(record);
        }
      }
    }
  }

  /**
   * Comparator for sorting variants by genomic position and alternateBases
   */
  public static class VariantComparator implements Comparator<Variant> {
    @Override
    public int compare(Variant v1, Variant v2) {

      int startComparison = v1.getStart().compareTo(v2.getStart());
      if (0 != startComparison) {
        return startComparison;
      }

      if (null == v1.getAlternateBases()) {
        return -1;
      } else if (null == v2.getAlternateBases()) {
        return 1;
      }
      return 0;
    }
  }

  public static boolean isOverlapping(Variant blockRecord, Variant variant) {
    if (blockRecord.getStart() <= variant.getStart()
        && blockRecord.getEnd() >= variant.getStart() + 1) {
      return true;
    }
    return false;
  }
}
