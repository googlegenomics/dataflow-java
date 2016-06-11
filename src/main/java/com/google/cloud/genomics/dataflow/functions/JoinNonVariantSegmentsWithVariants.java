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
import com.google.cloud.genomics.utils.grpc.MergeNonVariantSegmentsWithSnps;
import com.google.cloud.genomics.utils.grpc.VariantEmitterStrategy;
import com.google.cloud.genomics.utils.grpc.VariantMergeStrategy;
import com.google.cloud.genomics.utils.grpc.VariantStreamIterator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.StreamVariantsResponse;
import com.google.genomics.v1.Variant;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

    @Description("The class that determines the strategy for merging non-variant segments and variants.")
    @Default.Class(MergeNonVariantSegmentsWithSnps.class)
    Class<? extends VariantMergeStrategy> getVariantMergeStrategy();
    void setVariantMergeStrategy(Class<? extends VariantMergeStrategy> mergeStrategy);

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
        int binSize = options.getBinSize();
        Variant variant = context.element();
        long startBin = getStartBin(binSize, variant);
        long endBin = getEndBin(binSize, variant);
        for (long bin = startBin; bin <= endBin; bin++) {
          context.output(KV.of(KV.of(variant.getReferenceName(), bin * binSize), variant));
        }
      }
    }
  }

  /**
   * Use this transform when working with a collection of sites across the genome.
   *
   * It passes the data onto the next step retaining the ordering imposed by the
   * genomics API which is sorted by (variantset id, contig, start pos, variant id).
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

    public static final class RetrieveFn extends DoFn<StreamVariantsRequest, KV<KV<String, Long>, Iterable<Variant>>> {
      private final OfflineAuth auth;
      private String fields;

      public RetrieveFn(OfflineAuth auth, String fields) {
        super();
        this.auth = auth;
        this.fields = fields;
      }

      @Override
      public void processElement(DoFn<StreamVariantsRequest, KV<KV<String, Long>, Iterable<Variant>>>.ProcessContext context)
          throws Exception {
        StreamVariantsRequest request = context.element();

        Iterator<StreamVariantsResponse> iter = VariantStreamIterator.enforceShardBoundary(auth, request,
            ShardBoundary.Requirement.OVERLAPS, fields);

        if (iter.hasNext()) {
          // We do have some data overlapping this site.
          List<Iterable<Variant>> allVariantsForRequest = new ArrayList<>();
          while (iter.hasNext()) {
            allVariantsForRequest.add(iter.next().getVariantsList());
          }
          context.output(KV.of(KV.of(request.getReferenceName(), request.getStart()), Iterables.concat(allVariantsForRequest)));
        }
      }
    }
  }

  public static final class CombineVariantsFn extends DoFn<KV<KV<String, Long>, Iterable<Variant>>, Variant> {
    private VariantMergeStrategy merger;

    @Override
    public void startBundle(DoFn<KV<KV<String, Long>, Iterable<Variant>>, Variant>.Context c) throws Exception {
      super.startBundle(c);
      Options options = c.getPipelineOptions().as(Options.class);
      merger = options.getVariantMergeStrategy().newInstance();
    }

    @Override
    public void processElement(ProcessContext context) throws Exception {
      merger.merge(context.element().getKey().getValue(), context.element().getValue(), new DataflowVariantEmitter(context));
    }
  }

  public static class DataflowVariantEmitter implements VariantEmitterStrategy {
    private final DoFn<KV<KV<String, Long>, Iterable<Variant>>, Variant>.ProcessContext context;

    public DataflowVariantEmitter(DoFn<KV<KV<String, Long>, Iterable<Variant>>, Variant>.ProcessContext context) {
      this.context = context;
    }

    @Override
    public void emit(Variant variant) {
      context.output(variant);
    }
  }
}
