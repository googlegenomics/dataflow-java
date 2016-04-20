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
import java.util.LinkedList;
import java.util.List;

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.grpc.VariantStreamIterator;
import com.google.cloud.genomics.utils.grpc.VariantUtils;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.StreamVariantsResponse;
import com.google.genomics.v1.Variant;
import com.google.genomics.v1.Variant.Builder;

/**
 * The transforms in this class convert data with non-variant segments (such as data that was in
 * source format Genome VCF (gVCF) or Complete Genomics) to variant-only data with calls from
 * non-variant-segments merged into the variants with which they overlap.
 *
 * This is currently done only for SNP variants. Indels and structural variants are left as-is.
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
   * Use this transform when working with entire chromosomes or the whole genome.
   *
   * This transform assumes the use of STRICT shard boundary for the variant retrieval that has occurred
   * upstream so that no duplicate records will occur within a "bin".
   *
   * The amount of RAM needed during the combine step is controlled by the --binSize option.
   *
   * Compared to the RetrieveAndCombineTransform, this transform has:
   * PROS
   * - separate control of shard size for data retrieved from the Genomics API verus bin size
   *   over which we combine variants
   * - upstream we were able to perform fewer requests to Genomics API since each stream (shard)
   *   can be very large
   * - less total data pulled from Genomics API, since redundant data is only pulled at shard
   *    boundaries and there are fewer of those
   * CONS
   * - uses a shuffle
   */
  public static class BinShuffleAndCombineTransform extends PTransform<PCollection<Variant>, PCollection<Variant>> {

    /**
     * @param input PCollection of variants to process.
     * @return PCollection of variant-only Variant objects with calls from non-variant-segments
     *     merged into the SNP variants with which they overlap.
     */
    @Override
    public PCollection<Variant> apply(PCollection<Variant> input) {
      return input
          .apply(ParDo.of(new BinVariantsFn()))
          .apply(GroupByKey.<KV<String, Long>, Variant>create())
          .apply(ParDo.of(new RetrieveWindowOfVariantsFn()))
          .apply(ParDo.of(new CombineVariantsFn()));
    }

    static final class BinVariantsFn extends DoFn<Variant, KV<KV<String, Long>, Variant>> {

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

    static final class RetrieveWindowOfVariantsFn extends
    DoFn<KV<KV<String, Long>, Iterable<Variant>>, Iterable<Variant>> {

      @Override
      public void processElement(ProcessContext context) {

        // The upper bound on number of variants in the iterable is dependent upon the binSize
        // used in the prior step to construct the key.
        KV<KV<String, Long>, Iterable<Variant>> kv = context.element();
        context.output(kv.getValue());
      }
    }
  }

  /**
   * Use this transform when working with a collection of sites across the genome.
   *
   * The amount of RAM needed during the combine step is controlled by the number of
   * base pairs between the start and end position of each site.
   * Compared to the BinShuffleAndCombineTransform, this transform has:
   * PROS
   * - no shuffle!
   * CONS
   * - more requests to the Genomics API since we have a separate stream per site
   * - potentially more duplicate data pulled if the sites are near each other
   */
  public static class RetrieveAndCombineTransform extends PTransform<PCollection<StreamVariantsRequest>, PCollection<Variant>> {
    private final OfflineAuth auth;
    private String fields;

    /**
     * @param auth The OfflineAuth to use for the request.
     * @param fields Which fields to include in a partial response or null for all.
     */
    public RetrieveAndCombineTransform(OfflineAuth auth, String fields) {
      super();
      this.auth = auth;
      this.fields = fields;
    }

    @Override
    public PCollection<Variant> apply(PCollection<StreamVariantsRequest> input) {
      return input
          .apply(ParDo.of(new RetrieveFn(auth, fields)))
          .apply(ParDo.of(new CombineVariantsFn()));
    }

    static final class RetrieveFn extends DoFn<StreamVariantsRequest, Iterable<Variant>> {
      private final OfflineAuth auth;
      private String fields;

      public RetrieveFn(OfflineAuth auth, String fields) {
        super();
        this.auth = auth;
        this.fields = fields;
      }

      @Override
      public void processElement(DoFn<StreamVariantsRequest, Iterable<Variant>>.ProcessContext context)
          throws Exception {

        Iterator<StreamVariantsResponse> iter = VariantStreamIterator.enforceShardBoundary(auth, context.element(),
            ShardBoundary.Requirement.NON_VARIANT_OVERLAPS, fields);

        if (iter.hasNext()) {
          // We do have some data overlapping this site.
          List<Iterable<Variant>> allVariantsForRequest = new ArrayList<>();
          while (iter.hasNext()) {
            allVariantsForRequest.add(iter.next().getVariantsList());
          }
          context.output(Iterables.concat(allVariantsForRequest));
        }
      }
    }
  }

  /**
   * This DoFn converts data with non-variant segments (such as data that was in
   * source format Genome VCF (gVCF) or Complete Genomics) to variant-only data with calls from
   * non-variant-segments merged into the variants with which they overlap.
   *
   * This is currently done only for SNP variants. Indels and structural variants are left as-is.
   */
  public static final class CombineVariantsFn extends DoFn<Iterable<Variant>, Variant> {

    /**
     * Dev note: this code aims to minimize the amount of data held in memory.  It should only
     * be the current variant we are considering and any non-variant segments that overlap it.
     */
    @Override
    public void processElement(ProcessContext context) throws Exception {
      List<Variant> records = Lists.newArrayList(context.element());

      // The sort order is critical here so that candidate overlapping reference matching blocks
      // occur prior to any variants they may overlap.
      Collections.sort(records, NON_VARIANT_SEGMENT_COMPARATOR);

      // The upper bound on potential overlaps is the sample size plus the number of
      // block records that occur between actual variants.
      List<Variant> blockRecords = new LinkedList<>();

      for (Variant record : records) {
        if (!VariantUtils.IS_NON_VARIANT_SEGMENT.apply(record)) {
          // Dataflow does not allow the output of modified input items, so we make a copy and
          // modify that, if applicable.
          Builder updatedRecord = Variant.newBuilder(record);
          // TODO: determine and implement the correct criteria for overlaps of non-SNP variants
          if (VariantUtils.IS_SNP.apply(record)) {
            for (Iterator<Variant> iterator = blockRecords.iterator(); iterator.hasNext();) {
              Variant blockRecord = iterator.next();
              if (isOverlapping(blockRecord, record)) {
                updatedRecord.addAllCalls(blockRecord.getCallsList());
              } else {
                // Remove the current element from the iterator and the list since it is
                // left of the genomic region we are currently working on due to our sort.
                iterator.remove();
              }
            }
          }
          // Emit this variant and move on (no need to hang onto it in memory).
          context.output(updatedRecord.build());
        } else {
          blockRecords.add(record);
        }
      }
    }

    static final Ordering<Variant> BY_START = Ordering.natural().onResultOf(
        new Function<Variant, Long>() {
          @Override
          public Long apply(Variant variant) {
            return variant.getStart();
          }
        });

    static final Ordering<Variant> BY_FIRST_OF_ALTERNATE_BASES = Ordering.natural()
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
    static final Comparator<Variant> NON_VARIANT_SEGMENT_COMPARATOR = BY_START
        .compound(BY_FIRST_OF_ALTERNATE_BASES);

    static final boolean isOverlapping(Variant blockRecord, Variant variant) {
      return blockRecord.getStart() <= variant.getStart()
          && blockRecord.getEnd() >= variant.getStart() + 1;
    }
  }
}
