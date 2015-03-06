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
package com.google.cloud.genomics.dataflow.pipelines;

import com.google.api.client.util.Strings;
import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.Annotation;
import com.google.api.services.genomics.model.AnnotationSet;
import com.google.api.services.genomics.model.ListBasesResponse;
import com.google.api.services.genomics.model.QueryRange;
import com.google.api.services.genomics.model.RangePosition;
import com.google.api.services.genomics.model.SearchAnnotationsRequest;
import com.google.api.services.genomics.model.Variant;
import com.google.api.services.genomics.model.VariantAnnotation;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.utils.AnnotationUtils;
import com.google.cloud.genomics.dataflow.utils.AnnotationUtils.VariantEffect;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.VariantUtils;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.Contig.SexChromosomeFilter;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.Paginator;
import com.google.cloud.genomics.utils.Paginator.ShardBoundary;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;

import htsjdk.samtools.util.IntervalTree;
import htsjdk.samtools.util.IntervalTree.Node;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Demonstrates a simple variant annotation program which takes
 * {@code VariantSet}s and {@code AnnotationSet}s as input and emits
 * {@code VariantAnnotation}s. This program is intended to serve as an example
 * of how a variant annotation program can be structured to run in parallel,
 * operating over Google Genomics input sources; the current output has limited
 * biological significance.
 *
 * Currently only annotates SNPs and uses the following two methods:
 * <ul>
 * <li>Determines the effect of the provided variants on the provided
 *   transcripts, if any. A result is only emitted for a subset of cases where
 *   the variant appears to cause a coding change in the transcript.
 * <li>Performs an exact match join on the provided existing variant
 *   annotations, if any.
 * </ul>
 */
public final class AnnotateVariants extends DoFn<Contig, KV<String, VariantAnnotation>> {
  private static final Logger LOG = Logger.getLogger(AnnotateVariants.class.getName());
  private static final int VARIANTS_PAGE_SIZE = 5000;
  private static final String VARIANT_FIELDS
      = "nextPageToken,variants(id,referenceName,start,end,alternateBases,referenceBases)";

  private final GenomicsFactory.OfflineAuth auth;
  private final String varsetId;
  private final List<String> callSetIds, transcriptSetIds, variantAnnotationSetIds;
  private final Map<Range<Long>, String> refBaseCache;

  public AnnotateVariants(GenomicsFactory.OfflineAuth auth, String varsetId,
      List<String> callSetIds, List<String> transcriptSetIds,
      List<String> variantAnnotationSetIds) {
    this.auth = auth;
    this.varsetId = varsetId;
    this.callSetIds = callSetIds;
    this.transcriptSetIds = transcriptSetIds;
    this.variantAnnotationSetIds = variantAnnotationSetIds;
    refBaseCache = Maps.newHashMap();
  }

  @Override
  public void processElement(
      DoFn<Contig, KV<String, VariantAnnotation>>.ProcessContext c) throws Exception {
    Genomics genomics = auth.getGenomics(auth.getDefaultFactory());

    Contig contig = c.element();
    LOG.info("processing contig " + contig);
    Iterable<Variant> varIter = FluentIterable
        .from(Paginator.Variants.create(genomics, ShardBoundary.STRICT).search(
            contig.getVariantsRequest(varsetId)
              // TODO: Variants-only retrieval is not well support currently. For now
              // we parameterize by CallSet for performance.
              .setCallSetIds(callSetIds)
              .setPageSize(VARIANTS_PAGE_SIZE),
            VARIANT_FIELDS))
        .filter(VariantUtils.IS_SNP);
    if (!varIter.iterator().hasNext()) {
      LOG.info("region has no variants, skipping");
      return;
    }

    IntervalTree<Annotation> transcripts = retrieveTranscripts(genomics, contig);
    ListMultimap<Range<Long>, Annotation> variantAnnotations =
        retrieveVariantAnnotations(genomics, contig);

    Stopwatch stopwatch = Stopwatch.createStarted();
    int varCount = 0;
    for (Variant variant : varIter) {
      List<String> alleles = ImmutableList.<String>builder()
          .addAll(variant.getAlternateBases())
          .add(variant.getReferenceBases())
          .build();
      Range<Long> pos = Range.openClosed(variant.getStart(), variant.getEnd());
      for (String allele : alleles) {
        String outKey = Joiner.on(":").join(
            variant.getReferenceName(), variant.getStart(), allele, variant.getId());
        for (Annotation match : variantAnnotations.get(pos)) {
          if (allele.equals(match.getVariant().getAlternateBases())) {
            // Exact match to a known variant annotation; straightforward join.
            c.output(KV.of(outKey, match.getVariant()));
          }
        }

        Iterator<Node<Annotation>> transcriptIter = transcripts.overlappers(
            pos.lowerEndpoint().intValue(), pos.upperEndpoint().intValue() - 1); // Inclusive.
        while (transcriptIter.hasNext()) {
          // Calculate an effect of this allele on the coding region of the given transcript.
          Annotation transcript = transcriptIter.next().getValue();
          VariantEffect effect = AnnotationUtils.determineVariantTranscriptEffect(
              variant.getStart(), allele, transcript,
              getCachedTranscriptBases(genomics, transcript));
          if (effect != null && !VariantEffect.SYNONYMOUS_SNP.equals(effect)) {
            c.output(KV.of(outKey, new VariantAnnotation()
                .setAlternateBases(allele)
                .setType("SNP")
                .setEffect(effect.toString())
                .setGeneId(transcript.getTranscript().getGeneId())
                .setTranscriptIds(ImmutableList.of(transcript.getId()))));
          }
        }
      }
      varCount++;
      if (varCount%1e3 == 0) {
        LOG.info(String.format("read %d variants (%.2f / s)",
            varCount, (double)varCount / stopwatch.elapsed(TimeUnit.SECONDS)));
      }
    }
    LOG.info("finished reading " + varCount + " variants in " + stopwatch);
  }

  private ListMultimap<Range<Long>, Annotation> retrieveVariantAnnotations(
      Genomics genomics, Contig contig) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    ListMultimap<Range<Long>, Annotation> annotationMap = ArrayListMultimap.create();
    Iterable<Annotation> annotationIter =
        Paginator.Annotations.create(genomics, ShardBoundary.OVERLAPS).search(
            new SearchAnnotationsRequest()
              .setAnnotationSetIds(variantAnnotationSetIds)
              .setRange(new QueryRange()
                .setReferenceName(canonicalizeRefName(contig.referenceName))
                .setStart(contig.start)
                .setEnd(contig.end)));
    for (Annotation annotation : annotationIter) {
      RangePosition pos = annotation.getPosition();
      long start = 0;
      if (pos.getStart() != null) {
        start = pos.getStart();
      }
      annotationMap.put(Range.closedOpen(start, pos.getEnd()), annotation);
    }
    LOG.info(String.format("read %d variant annotations in %s (%.2f / s)", annotationMap.size(),
        stopwatch, (double)annotationMap.size() / stopwatch.elapsed(TimeUnit.SECONDS)));
    return annotationMap;
  }

  private IntervalTree<Annotation> retrieveTranscripts(Genomics genomics, Contig contig) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    IntervalTree<Annotation> transcripts = new IntervalTree<>();
    Iterable<Annotation> transcriptIter =
        Paginator.Annotations.create(genomics, ShardBoundary.OVERLAPS).search(
            new SearchAnnotationsRequest()
              .setAnnotationSetIds(transcriptSetIds)
              .setRange(new QueryRange()
                .setReferenceName(canonicalizeRefName(contig.referenceName))
                .setStart(contig.start)
                .setEnd(contig.end)));
    for (Annotation annotation : transcriptIter) {
      RangePosition pos = annotation.getPosition();
      transcripts.put(pos.getStart().intValue(), pos.getEnd().intValue(), annotation);
    }
    LOG.info(String.format("read %d transcripts in %s (%.2f / s)", transcripts.size(),
        stopwatch, (double)transcripts.size() / stopwatch.elapsed(TimeUnit.SECONDS)));
    return transcripts;
  }

  private String getCachedTranscriptBases(Genomics genomics, Annotation transcript)
      throws IOException {
    RangePosition pos = transcript.getPosition();
    Range<Long> rng = Range.closedOpen(pos.getStart(), pos.getEnd());
    if (!refBaseCache.containsKey(rng)) {
      refBaseCache.put(rng, retrieveReferenceBases(genomics, pos));
    }
    return refBaseCache.get(rng);
  }

  private String retrieveReferenceBases(Genomics genomics, RangePosition pos) throws IOException {
    StringBuilder b = new StringBuilder();
    String pageToken = "";
    while (true) {
      // TODO: Support full request parameterization for Paginator.References.Bases.
      ListBasesResponse response = genomics.references().bases()
          .list(pos.getReferenceId())
          .setStart(pos.getStart())
          .setEnd(pos.getEnd())
          .setPageToken(pageToken)
          .execute();
      b.append(response.getSequence());
      pageToken = response.getNextPageToken();
      if (Strings.isNullOrEmpty(pageToken)) {
        break;
      }
    }
    return b.toString();
  }

  private static String canonicalizeRefName(String refName) {
    return refName.replace("chr", "");
  }

  public static void main(String[] args) throws Exception {
    GenomicsDatasetOptions opts = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(GenomicsDatasetOptions.class);
    GenomicsFactory.OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(opts);
    Genomics genomics = auth.getGenomics(auth.getDefaultFactory());

    List<String> callSetIds = ImmutableList.of();
    if (!Strings.isNullOrEmpty(opts.getCallSetIds().trim())) {
      callSetIds = ImmutableList.copyOf(opts.getCallSetIds().split(","));
    }
    List<String> transcriptSetIds =
        validateAnnotationSetsFlag(genomics, opts.getTranscriptSetIds(), "TRANSCRIPT");
    List<String> variantAnnotationSetIds =
        validateAnnotationSetsFlag(genomics, opts.getVariantAnnotationSetIds(), "VARIANT");
    validateRefsetForAnnotationSets(genomics, transcriptSetIds);

    Iterable<Contig> contigs = opts.isAllReferences()
        ? Contig.getContigsInVariantSet(
            genomics, opts.getDatasetId(), SexChromosomeFilter.INCLUDE_XY)
        : Contig.parseContigsFromCommandLine(opts.getReferences());

    Pipeline p = Pipeline.create(opts);
    DataflowWorkarounds.registerGenomicsCoders(p);
    p.begin()
      .apply(Create.of(contigs))
      .apply(ParDo.of(new AnnotateVariants(auth,
          opts.getDatasetId(), callSetIds, transcriptSetIds, variantAnnotationSetIds)))
      .apply(GroupByKey.<String, VariantAnnotation>create())
      .apply(ParDo.of(new DoFn<KV<String, Iterable<VariantAnnotation>>, String>() {
        @Override
        public void processElement(ProcessContext c) {
          c.output(c.element().getKey() + ": " + c.element().getValue());
        }
      }))
      .apply(TextIO.Write.to(opts.getOutput()));
    p.run();
  }

  private static void validateRefsetForAnnotationSets(
      Genomics genomics, List<String> annosetIds) throws IOException {
    String refsetId = null;
    for (String annosetId : annosetIds) {
      String gotId = genomics.annotationSets().get(annosetId).execute().getReferenceSetId();
      if (refsetId == null) {
        refsetId = gotId;
      } else if (!refsetId.equals(gotId)) {
        throw new IllegalArgumentException("want consistent reference sets across the provided " +
            "annotation sets, got " + refsetId + " and " + gotId);
      }
    }
  }

  private static List<String> validateAnnotationSetsFlag(
      Genomics genomics, String flagValue, String wantType) throws IOException {
    List<String> annosetIds = ImmutableList.copyOf(flagValue.split(","));
    for (String annosetId : annosetIds) {
      AnnotationSet annoset = genomics.annotationSets().get(annosetId).execute();
      if (!wantType.equals(annoset.getType())) {
        throw new IllegalArgumentException("annotation set " + annosetId + " has type " +
            annoset.getType() + ", wanted type " + wantType);
      }
    }
    return annosetIds;
  }
}
