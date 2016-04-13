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
package com.google.cloud.genomics.dataflow.functions.grpc;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.utils.grpc.VariantUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.genomics.v1.Variant;
import com.google.genomics.v1.Variant.Builder;

/**
 * The transforms in this class convert data with non-variant segments (such as data that was in
 * source format Genome VCF (gVCF) or Complete Genomics) to variant-only data with calls from
 * non-variant-segments merged into the variants with which they overlap.
 * 
 * This is currently done only for SNP variants. Indels and structural variants are left as-is.
 * 
 * SCALING: For a large cohort with many, many more rare variants we may wish to instead modify the
 * logic here to summarize the number of calls that match the reference for each variant instead of
 * adding those individual calls to the record.
 * 
 */
public class JoinNonVariantSegmentsWithVariants {
  
  public static interface Options extends PipelineOptions {
    @Description("If querying a dataset with non-variant segments (such as Complete Genomics data "
        + "or data in Genome VCF (gVCF) format), specify this flag so that the pipeline correctly "
        + "takes into account non-variant segment records that overlap variants within the dataset.")
    @Default.Boolean(false)
    boolean getHasNonVariantSegments();
    void setHasNonVariantSegments(boolean hasNonVariantSegments);

    @Description("Genomic window \"bin\" size to use for data containing non-variant segments when "
        + "joining those non-variant segment records with variant records.")
    @Default.Integer(1000)
    int getBinSize();
    void setBinSize(int binSize);

    public static class Methods {
      public static void validateOptions(Options options) {
        Preconditions.checkArgument(0 < options.getBinSize(), "binSize must be greater than zero");
      }
    }
  }

  /**
   * Use this transform for correct handling of data with non-variant segments in DataFlow jobs that
   * consider not only calls that have the variant but also those that match the reference at that
   * variant position.
   * 
   * @param input PCollection of variants to process.
   * @return PCollection of variant-only Variant objects with calls from non-variant-segments
   *     merged into the variants with which they overlap.
   */
  public static PCollection<Variant> joinVariantsTransform(PCollection<Variant> input) {
    return joinVariants(input);
  }

  private static PCollection<Variant> joinVariants(PCollection<Variant> input) {
    return input
        .apply(ParDo.of(new JoinNonVariantSegmentsWithVariants.BinVariants()))
        .apply(GroupByKey.<KV<String, Long>, Variant>create())
        .apply(ParDo.of(new JoinNonVariantSegmentsWithVariants.MergeVariants()));
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
          if (null == variant.getAlternateBasesList() || variant.getAlternateBasesList().isEmpty()) {
            return null;
          }
          return variant.getAlternateBases(0);
        }
      });

  // Special-purpose comparator for use in dealing with both variant and non-variant segment data.
  // Sort by start position ascending and ensure that if a variant and a ref-matching block are at
  // the same position, the non-variant segment record comes first.
  @VisibleForTesting
  static final Comparator<Variant> NON_VARIANT_SEGMENT_COMPARATOR = BY_START
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
      Options options =
          context.getPipelineOptions().as(Options.class);
      Variant variant = context.element();
      long startBin = getStartBin(options.getBinSize(), variant);
      long endBin =
          VariantUtils.IS_NON_VARIANT_SEGMENT.apply(variant) ? getEndBin(options.getBinSize(),
              variant) : startBin;
      for (long bin = startBin; bin <= endBin; bin++) {
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
      Collections.sort(records, NON_VARIANT_SEGMENT_COMPARATOR);

      // The upper bound on potential overlaps is the sample size plus the number of
      // block records that occur between actual variants.
      List<Variant> blockRecords = new LinkedList<>();

      for (Variant record : records) {
        if (!VariantUtils.IS_NON_VARIANT_SEGMENT.apply(record)) {
          Builder updatedRecord = Variant.newBuilder(record);
          // TODO: determine and implement the correct criteria for overlaps of non-SNP variants
          if (VariantUtils.IS_SNP.apply(record)) {
            for (Iterator<Variant> iterator = blockRecords.iterator(); iterator.hasNext();) {
              Variant blockRecord = iterator.next();
              if (JoinNonVariantSegmentsWithVariants.isOverlapping(blockRecord, record)) {
                updatedRecord.addAllCalls(blockRecord.getCallsList());
              } else {
                // Remove the current element from the iterator and the list since it is
                // left of the genomic region we are currently working on due to our sort..
                iterator.remove();
              }
            }
          }
          context.output(updatedRecord.build());
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
