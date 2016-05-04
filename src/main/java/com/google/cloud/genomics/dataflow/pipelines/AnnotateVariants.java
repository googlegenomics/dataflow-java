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
import com.google.api.services.genomics.model.SearchAnnotationsRequest;
import com.google.api.services.genomics.model.VariantAnnotation;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.utils.AnnotationUtils;
import com.google.cloud.genomics.dataflow.utils.AnnotationUtils.VariantEffect;
import com.google.cloud.genomics.dataflow.utils.GCSOutputOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.Paginator;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.cloud.genomics.utils.grpc.VariantStreamIterator;
import com.google.cloud.genomics.utils.grpc.VariantUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.StreamVariantsResponse;
import com.google.genomics.v1.Variant;

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
 *
 * See http://googlegenomics.readthedocs.org/en/latest/use_cases/annotate_variants/google_genomics_annotation.html
 * for running instructions.
 */
public final class AnnotateVariants extends DoFn<StreamVariantsRequest, KV<String, VariantAnnotation>> {

  public static interface Options extends ShardOptions, GCSOutputOptions {

    @Description("The ID of the Google Genomics variant set this pipeline is accessing. "
        + "Defaults to 1000 Genomes.")
    @Default.String("10473108253681171589")
    String getVariantSetId();

    void setVariantSetId(String variantSetId);

    @Description("The IDs of the Google Genomics call sets this pipeline is working with, comma "
        + "delimited.Defaults to 1000 Genomes HG00261.")
    @Default.String("10473108253681171589-0")
    String getCallSetIds();
    void setCallSetIds(String callSetIds);

    @Description("The IDs of the Google Genomics transcript sets this pipeline is working with, "
        + "comma delimited. Defaults to UCSC refGene (hg19).")
    @Default.String("CIjfoPXj9LqPlAEQ5vnql4KewYuSAQ")
    String getTranscriptSetIds();
    void setTranscriptSetIds(String transcriptSetIds);

    @Description("The IDs of the Google Genomics variant annotation sets this pipeline is working "
        + "with, comma delimited. Defaults to ClinVar (GRCh37).")
    @Default.String("CILSqfjtlY6tHxC0nNH-4cu-xlQ")
    String getVariantAnnotationSetIds();
    void setVariantAnnotationSetIds(String variantAnnotationSetIds);

    public static class Methods {
      public static void validateOptions(Options options) {
        GCSOutputOptions.Methods.validateOptions(options);
      }
    }

  }

  private static final Logger LOG = Logger.getLogger(AnnotateVariants.class.getName());
  private static final int VARIANTS_PAGE_SIZE = 5000;
  // Tip: Use the API explorer to test which fields to include in partial responses.
  // https://developers.google.com/apis-explorer/#p/genomics/v1/genomics.variants.stream?fields=variants(alternateBases%252Ccalls(callSetName%252Cgenotype)%252CreferenceBases)&_h=3&resource=%257B%250A++%2522variantSetId%2522%253A+%25223049512673186936334%2522%252C%250A++%2522referenceName%2522%253A+%2522chr17%2522%252C%250A++%2522start%2522%253A+%252241196311%2522%252C%250A++%2522end%2522%253A+%252241196312%2522%252C%250A++%2522callSetIds%2522%253A+%250A++%255B%25223049512673186936334-0%2522%250A++%255D%250A%257D&
  private static final String VARIANT_FIELDS
      = "variants(id,referenceName,start,end,alternateBases,referenceBases)";

  private final OfflineAuth auth;
  private final List<String> callSetIds, transcriptSetIds, variantAnnotationSetIds;
  private final Map<Range<Long>, String> refBaseCache;

  public AnnotateVariants(OfflineAuth auth,
      List<String> callSetIds, List<String> transcriptSetIds,
      List<String> variantAnnotationSetIds) {
    this.auth = auth;
    this.callSetIds = callSetIds;
    this.transcriptSetIds = transcriptSetIds;
    this.variantAnnotationSetIds = variantAnnotationSetIds;
    refBaseCache = Maps.newHashMap();
  }

  @Override
  public void processElement(
      DoFn<StreamVariantsRequest, KV<String, VariantAnnotation>>.ProcessContext c) throws Exception {
    Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);

    StreamVariantsRequest request = StreamVariantsRequest.newBuilder(c.element())
        .addAllCallSetIds(callSetIds)
        .build();
    LOG.info("processing contig " + request);

    Iterator<StreamVariantsResponse> iter = VariantStreamIterator.enforceShardBoundary(auth,
        request, ShardBoundary.Requirement.STRICT, VARIANT_FIELDS);
    if (!iter.hasNext()) {
      LOG.info("region has no variants, skipping");
      return;
    }

    IntervalTree<Annotation> transcripts = retrieveTranscripts(genomics, request);
    ListMultimap<Range<Long>, Annotation> variantAnnotations =
        retrieveVariantAnnotations(genomics, request);

    Stopwatch stopwatch = Stopwatch.createStarted();
    int varCount = 0;
    while (iter.hasNext()) {
      Iterable<Variant> varIter = FluentIterable
          .from(iter.next().getVariantsList())
          .filter(VariantUtils.IS_SNP);
      for (Variant variant : varIter) {
        List<String> alleles = ImmutableList.<String>builder()
            .addAll(variant.getAlternateBasesList())
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
    }
    LOG.info("finished reading " + varCount + " variants in " + stopwatch);
  }

  private ListMultimap<Range<Long>, Annotation> retrieveVariantAnnotations(
      Genomics genomics, StreamVariantsRequest request) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    ListMultimap<Range<Long>, Annotation> annotationMap = ArrayListMultimap.create();
    Iterable<Annotation> annotationIter =
        Paginator.Annotations.create(genomics, ShardBoundary.Requirement.OVERLAPS).search(
            new SearchAnnotationsRequest()
              .setAnnotationSetIds(variantAnnotationSetIds)
              .setReferenceName(canonicalizeRefName(request.getReferenceName()))
              .setStart(request.getStart())
              .setEnd(request.getEnd()));
    for (Annotation annotation : annotationIter) {
      long start = 0;
      if (annotation.getStart() != null) {
        start = annotation.getStart();
      }
      annotationMap.put(Range.closedOpen(start, annotation.getEnd()), annotation);
    }
    LOG.info(String.format("read %d variant annotations in %s (%.2f / s)", annotationMap.size(),
        stopwatch, (double)annotationMap.size() / stopwatch.elapsed(TimeUnit.SECONDS)));
    return annotationMap;
  }

  private IntervalTree<Annotation> retrieveTranscripts(Genomics genomics, StreamVariantsRequest request) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    IntervalTree<Annotation> transcripts = new IntervalTree<>();
    Iterable<Annotation> transcriptIter =
        Paginator.Annotations.create(genomics, ShardBoundary.Requirement.OVERLAPS).search(
            new SearchAnnotationsRequest()
              .setAnnotationSetIds(transcriptSetIds)
              .setReferenceName(canonicalizeRefName(request.getReferenceName()))
              .setStart(request.getStart())
              .setEnd(request.getEnd()));
    for (Annotation annotation : transcriptIter) {
      transcripts.put(annotation.getStart().intValue(), annotation.getEnd().intValue(), annotation);
    }
    LOG.info(String.format("read %d transcripts in %s (%.2f / s)", transcripts.size(),
        stopwatch, (double)transcripts.size() / stopwatch.elapsed(TimeUnit.SECONDS)));
    return transcripts;
  }

  private String getCachedTranscriptBases(Genomics genomics, Annotation transcript)
      throws IOException {
    Range<Long> rng = Range.closedOpen(transcript.getStart(), transcript.getEnd());
    if (!refBaseCache.containsKey(rng)) {
      refBaseCache.put(rng, retrieveReferenceBases(genomics, transcript));
    }
    return refBaseCache.get(rng);
  }

  private String retrieveReferenceBases(Genomics genomics, Annotation annotation) throws IOException {
    StringBuilder b = new StringBuilder();
    String pageToken = "";
    while (true) {
      // TODO: Support full request parameterization for Paginator.References.Bases.
      ListBasesResponse response = genomics.references().bases()
          .list(annotation.getReferenceId())
          .setStart(annotation.getStart())
          .setEnd(annotation.getEnd())
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
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(Options.class);
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    Options.Methods.validateOptions(options);

    OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);

    List<String> callSetIds = ImmutableList.of();
    if (!Strings.isNullOrEmpty(options.getCallSetIds().trim())) {
      callSetIds = ImmutableList.copyOf(options.getCallSetIds().split(","));
    }
    List<String> transcriptSetIds =
        validateAnnotationSetsFlag(genomics, options.getTranscriptSetIds(), "TRANSCRIPT");
    List<String> variantAnnotationSetIds =
        validateAnnotationSetsFlag(genomics, options.getVariantAnnotationSetIds(), "VARIANT");
    validateRefsetForAnnotationSets(genomics, transcriptSetIds);

    List<StreamVariantsRequest> requests = options.isAllReferences() ?
        ShardUtils.getVariantRequests(options.getVariantSetId(), ShardUtils.SexChromosomeFilter.EXCLUDE_XY,
            options.getBasesPerShard(), auth) :
              ShardUtils.getVariantRequests(options.getVariantSetId(), options.getReferences(), options.getBasesPerShard());

    Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);

    p.begin()
      .apply(Create.of(requests))
      .apply(ParDo.of(new AnnotateVariants(auth, callSetIds, transcriptSetIds, variantAnnotationSetIds)))
      .apply(GroupByKey.<String, VariantAnnotation>create())
      .apply(ParDo.of(new DoFn<KV<String, Iterable<VariantAnnotation>>, String>() {
        @Override
        public void processElement(ProcessContext c) {
          c.output(c.element().getKey() + ": " + c.element().getValue());
        }
      }))
      .apply(TextIO.Write.to(options.getOutput()));
    p.run();
  }

  private static void validateRefsetForAnnotationSets(
      Genomics genomics, List<String> annosetIds) throws IOException {
    String refsetId = null;
    for (String annosetId : annosetIds) {
      String gotId = genomics.annotationsets().get(annosetId).execute().getReferenceSetId();
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
      AnnotationSet annoset = genomics.annotationsets().get(annosetId).execute();
      if (!wantType.equals(annoset.getType())) {
        throw new IllegalArgumentException("annotation set " + annosetId + " has type " +
            annoset.getType() + ", wanted type " + wantType);
      }
    }
    return annosetIds;
  }
}
