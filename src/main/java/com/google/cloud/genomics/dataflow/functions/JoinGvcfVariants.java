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

import static com.google.common.collect.Lists.newArrayList;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.api.client.util.Lists;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.api.services.genomics.model.Variant;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.readers.VariantReader;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.VariantUtils;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.common.base.Preconditions;

/**
 * The transforms in this class convert data in Genome VCF (gVCF) format to variant-only VCF data
 * with calls from block-records merged into the variants with which they overlap.
 * 
 * This is currently done only for SNP variants. Indels and structural variants are left as-is.
 */
public class JoinGvcfVariants {

  public static final String GVCF_VARIANT_FIELDS =
      "nextPageToken,variants(referenceName,start,end,referenceBases,alternateBases,calls(genotype,callSetName))";

  private static final List<String> REQUIRED_FIELDS = newArrayList("nextPageToken",
      "referenceName", "start", "end", "referenceBases", "alternateBases");

  /**
   * Use this PTransform for correct handling of gVCF data in DataFlow jobs that consider not only
   * calls that have the variant but also those that match the reference at that variant position.
   * 
   * All variant fields will be returned.
   * 
   * @param input
   * @param auth
   * @return
   */
  public static PCollection<Variant> joinGvcfVariantsTransform(
      PCollection<SearchVariantsRequest> input, GenomicsFactory.OfflineAuth auth) {
    return joinGvcfVariants(input, auth, null);
  }

  /**
   * Use this PTransform for correct handling of gVCF data in DataFlow jobs that consider not only
   * calls that have the variant but also those that match the reference at that variant position.
   * 
   * @param input
   * @param auth
   * @param fields Fields to be returned by the partial response.
   * @return
   */
  public static PCollection<Variant> joinGvcfVariantsTransform(
      PCollection<SearchVariantsRequest> input, GenomicsFactory.OfflineAuth auth, String fields) {
    for (String field : REQUIRED_FIELDS) {
      Preconditions.checkState(fields.contains(field), "Required field missing: " + field
          + "Add this field to the list of Variants fields returned in the partial response.");
    }
    return joinGvcfVariants(input, auth, fields);
  }

  private static PCollection<Variant> joinGvcfVariants(PCollection<SearchVariantsRequest> input,
      GenomicsFactory.OfflineAuth auth, String fields) {
    return input
        .apply(ParDo.named(VariantReader.class.getSimpleName()).of(new VariantReader(auth, fields)))
        .apply(ParDo.of(new JoinGvcfVariants.BinVariants()))
        // TODO check that windowing function is not splitting these groups
        .apply(GroupByKey.<KV<String, Long>, Variant>create())
        .apply(ParDo.of(new JoinGvcfVariants.MergeVariants()));
  }

  @SuppressWarnings("serial")
  public static class BinVariants extends DoFn<Variant, KV<KV<String, Long>, Variant>> {

    public long getStartBin(int binSize, Variant variant) {
      // Round down to the nearest integer
      return Math.round(Math.floor(variant.getStart() / binSize));
    }

    public long getEndBin(int binSize, Variant variant) {
      // Round down to the nearest integer
      return Math.round(Math.floor(variant.getEnd() / binSize));
    }

    @Override
    public void processElement(ProcessContext context) {
      GenomicsDatasetOptions options =
          context.getPipelineOptions().as(GenomicsDatasetOptions.class);
      Variant variant = context.element();
      for (long startBin = getStartBin(options.getBinSize(), variant), endBin =
          VariantUtils.isVariant(variant) ? startBin : getEndBin(options.getBinSize(), variant), bin =
          startBin; bin <= endBin; bin++) {
        context.output(KV.of(KV.of(variant.getReferenceName(), bin), variant));
      }
    }
  }

  @SuppressWarnings("serial")
  public static class MergeVariants extends DoFn<KV<KV<String, Long>, Iterable<Variant>>, Variant> {

    @Override
    public void processElement(ProcessContext context) {

      // The upper bound on number of variants in the iterable is dependent upon the binSize
      // used in the prior step to construct the key.
      KV<KV<String, Long>, Iterable<Variant>> kv = context.element();

      // Sort by start position descending and ensure that if a variant
      // and a ref-matching block are at the same position, the
      // ref-matching block comes first.
      List<Variant> records = Lists.newArrayList(kv.getValue());
      Collections.sort(records, new JoinGvcfVariants.VariantComparator());

      // The upper bound on potential overlaps is the sample size plus the number of
      // block records that occur between actual variants.
      List<Variant> blockRecords = new LinkedList<Variant>();

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
    return (blockRecord.getStart() <= variant.getStart() && blockRecord.getEnd() >= variant
        .getStart() + 1);
  }
}
