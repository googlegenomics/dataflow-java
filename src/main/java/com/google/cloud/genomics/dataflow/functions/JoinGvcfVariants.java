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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

/**
 * The transforms in this class convert data in Genome VCF (gVCF) format to variant-only VCF data
 * with calls from block-records merged into the variants with which they overlap.
 * 
 * This is currently done only for SNP variants. Indels and structural variants are left as-is.
 */
public class JoinGvcfVariants {

  public static final String GVCF_VARIANT_FIELDS =
      "nextPageToken,variants(referenceName,start,end,referenceBases,alternateBases,calls(genotype,callSetName))";

  private static final List<String> REQUIRED_FIELDS = Arrays.asList("nextPageToken",
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
      Preconditions
          .checkState(
              fields.contains(field),
              "Required field missing: %s Add this field to the list of Variants fields returned in the partial response.",
              field);
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

  private static final Ordering<Variant> BY_START = Ordering.natural().onResultOf(
      new Function<Variant, Long>() {
        @Override
        public Long apply(Variant variant) {
          return variant.getStart();
        }
      });

  private static final Ordering<Variant> BY_FIRST_OF_ALTERNATE_BASES = Ordering.natural()
      .nullsFirst().onResultOf(new Function<Variant, String>() {
        @Override
        public String apply(Variant variant) {
          if (null == variant.getAlternateBases() || variant.getAlternateBases().isEmpty()) {
            return null;
          }
          return variant.getAlternateBases().get(0);
        }
      });

  // Special-purpose comparator for use in dealing with GVCF data. Sort by start position ascending
  // and ensure that if a variant and a ref-matching block are at the same position, the
  // ref-matching block comes first.
  @VisibleForTesting
  static final Comparator<Variant> GVCF_VARIANT_COMPARATOR = BY_START
      .compound(BY_FIRST_OF_ALTERNATE_BASES);

  public static final class BinVariants extends DoFn<Variant, KV<KV<String, Long>, Variant>> {

    public static final long getStartBin(int binSize, Variant variant) {
      // Round down to the nearest integer
      return Math.round(Math.floor(variant.getStart() / binSize));
    }

    public static final long getEndBin(int binSize, Variant variant) {
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

  public static final class MergeVariants extends
      DoFn<KV<KV<String, Long>, Iterable<Variant>>, Variant> {

    @Override
    public void processElement(ProcessContext context) {

      // The upper bound on number of variants in the iterable is dependent upon the binSize
      // used in the prior step to construct the key.
      KV<KV<String, Long>, Iterable<Variant>> kv = context.element();

      List<Variant> records = Lists.newArrayList(kv.getValue());
      // The sort order is critical here so that candidate overlapping reference matching blocks
      // occur prior to any variants they may overlap.
      Collections.sort(records, GVCF_VARIANT_COMPARATOR);

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

  public static boolean isOverlapping(Variant blockRecord, Variant variant) {
    return blockRecord.getStart() <= variant.getStart()
        && blockRecord.getEnd() >= variant.getStart() + 1;
  }
}
