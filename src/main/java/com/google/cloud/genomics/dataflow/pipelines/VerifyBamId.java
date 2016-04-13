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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.api.client.util.Strings;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.functions.verifybamid.LikelihoodFn;
import com.google.cloud.genomics.dataflow.functions.verifybamid.ReadFunctions;
import com.google.cloud.genomics.dataflow.functions.verifybamid.Solver;
import com.google.cloud.genomics.dataflow.functions.verifybamid.VariantFunctions;
import com.google.cloud.genomics.dataflow.model.AlleleFreq;
import com.google.cloud.genomics.dataflow.model.ReadBaseQuality;
import com.google.cloud.genomics.dataflow.model.ReadBaseWithReference;
import com.google.cloud.genomics.dataflow.model.ReadCounts;
import com.google.cloud.genomics.dataflow.model.ReadQualityCount;
import com.google.cloud.genomics.dataflow.pipelines.CalculateCoverage.CheckMatchingReferenceSet;
import com.google.cloud.genomics.dataflow.readers.ReadGroupStreamer;
import com.google.cloud.genomics.dataflow.readers.VariantStreamer;
import com.google.cloud.genomics.dataflow.utils.GCSOutputOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.GenomicsUtils;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.cloud.genomics.utils.ShardUtils.SexChromosomeFilter;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.genomics.v1.Position;
import com.google.genomics.v1.Read;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.Variant;
import com.google.protobuf.ListValue;

/**
 * Test a set of reads for contamination.
 *
 * Takes a set of specified ReadGroupSets of reads to test and statistics on reference allele
 * frequencies for SNPs with a single alternative from a specified set of VariantSets.
 * 
 * See http://googlegenomics.readthedocs.org/en/latest/use_cases/perform_quality_control_checks/verify_bam_id.html
 * for running instructions.
 *
 * Uses the sequence data alone approach described in:
 * G. Jun, M. Flickinger, K. N. Hetrick, Kurt, J. M. Romm, K. F. Doheny,
 * G. Abecasis, M. Boehnke,and H. M. Kang, Detecting and Estimating
 * Contamination of Human DNA Samples in Sequencing and Array-Based Genotype
 * Data, American journal of human genetics doi:10.1016/j.ajhg.2012.09.004
 * (volume 91 issue 5 pp.839 - 848)
 * http://www.sciencedirect.com/science/article/pii/S0002929712004788
 */
public class VerifyBamId {

  /**
   * Options required to run this pipeline.
   */
  public static interface Options extends ShardOptions, GCSOutputOptions {

    @Description("A comma delimited list of the IDs of the Google Genomics ReadGroupSets this "
        + "pipeline is working with. Default (empty) indicates all ReadGroupSets in InputDatasetId."
        + "  This or InputDatasetId must be set.  InputDatasetId overrides "
        + "ReadGroupSetIds (if InputDatasetId is set, this field will be ignored).")
    @Default.String("")
    String getReadGroupSetIds();

    void setReadGroupSetIds(String readGroupSetId);

    @Description("The ID of the Google Genomics Dataset that the pipeline will get its input reads"
        + " from.  Default (empty) means to use ReadGroupSetIds and VariantSetIds instead.  This or"
        + " ReadGroupSetIds and VariantSetIds must be set.  InputDatasetId overrides"
        + " ReadGroupSetIds and VariantSetIds (if this field is set, ReadGroupSetIds and"
        + " VariantSetIds will be ignored).")
    @Default.String("")
    String getInputDatasetId();

    void setInputDatasetId(String inputDatasetId);

    public String DEFAULT_VARIANTSET = "10473108253681171589";
    @Description("The ID of the Google Genomics VariantSet this pipeline is working with."
        + "  It assumes the variant set has INFO field 'AF' from which it retrieves the"
        + " allele frequency for the variant, such as 1,000 Genomes phase 1 or phase 3 variants."
        + "  Defaults to the 1,000 Genomes phase 1 VariantSet with id " + DEFAULT_VARIANTSET + ".")
    @Default.String(DEFAULT_VARIANTSET)
    String getVariantSetId();

    void setVariantSetId(String variantSetId);

    @Description("The minimum allele frequency to use in analysis.  Defaults to 0.01.")
    @Default.Double(0.01)
    double getMinFrequency();

    void setMinFrequency(double minFrequency);

    @Description("The fraction of positions to check.  Defaults to 0.01.")
    @Default.Double(0.01)
    double getSamplingFraction();

    void setSamplingFraction(double minFrequency);

    public static class Methods {
      public static void validateOptions(Options options) {
        GCSOutputOptions.Methods.validateOptions(options);
      }
    }

  }

  private static Pipeline p;
  private static Options pipelineOptions;
  private static OfflineAuth auth;

  /**
   * String prefix used for sampling hash function
   */
  private static final String HASH_PREFIX = "";

  // TODO: this value is not quite correct. Test again after
  // https://github.com/googlegenomics/utils-java/issues/48
  private static final String VARIANT_FIELDS = "variants(start,calls(genotype,callSetName))";

  /**
   * Run the VerifyBamId algorithm and output the resulting contamination estimate.
   */
  public static void main(String[] args) throws GeneralSecurityException, IOException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(Options.class);
    pipelineOptions = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    Options.Methods.validateOptions(pipelineOptions);
    
    auth = GenomicsOptions.Methods.getGenomicsAuth(pipelineOptions);
    
    p = Pipeline.create(pipelineOptions);
    p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);

    if (pipelineOptions.getInputDatasetId().isEmpty() && pipelineOptions.getReadGroupSetIds().isEmpty()) {
      throw new IllegalArgumentException("InputDatasetId or ReadGroupSetIds must be specified");
    }

    List<String> rgsIds;
    if (pipelineOptions.getInputDatasetId().isEmpty()) {
      rgsIds = Lists.newArrayList(pipelineOptions.getReadGroupSetIds().split(","));
    } else {
      rgsIds = GenomicsUtils.getReadGroupSetIds(pipelineOptions.getInputDatasetId(), auth);
    }

    // Grab one ReferenceSetId to be used within the pipeline to confirm that all ReadGroupSets
    // are associated with the same ReferenceSet.
    String referenceSetId = GenomicsUtils.getReferenceSetId(rgsIds.get(0), auth);
    if (Strings.isNullOrEmpty(referenceSetId)) {
      throw new IllegalArgumentException("No ReferenceSetId associated with ReadGroupSetId "
          + rgsIds.get(0)
          + ". All ReadGroupSets in given input must have an associated ReferenceSet.");
    }

    // TODO: confirm that variant set also corresponds to the same reference
    // https://github.com/googlegenomics/api-client-java/issues/66
    
    // Reads in Reads.
    PCollection<Read> reads = p.begin()
        .apply(Create.of(rgsIds))
        .apply(ParDo.of(new CheckMatchingReferenceSet(referenceSetId, auth)))
        .apply(new ReadGroupStreamer(auth, ShardBoundary.Requirement.STRICT, null, SexChromosomeFilter.INCLUDE_XY));

    /*
    TODO:  We can reduce the number of requests needed to be created by doing the following:
    1. Stream the Variants first (rather than concurrently with the Reads).  Select a subset of
       them equal to some threshold (say 50K by default).
    2. Create the requests for streaming Reads by running a ParDo over the selected Variants
       to get their ranges (we only need to stream Reads that overlap the selected Variants).
    3. Stream the Reads from the created requests.
    */

    // Reads in Variants.  TODO potentially provide an option to load the Variants from a file.
    List<StreamVariantsRequest> variantRequests = pipelineOptions.isAllReferences() ?
        ShardUtils.getVariantRequests(pipelineOptions.getVariantSetId(), ShardUtils.SexChromosomeFilter.INCLUDE_XY,
            pipelineOptions.getBasesPerShard(), auth) :
          ShardUtils.getVariantRequests(pipelineOptions.getVariantSetId(), pipelineOptions.getReferences(), pipelineOptions.getBasesPerShard());

    PCollection<Variant> variants = p.apply(Create.of(variantRequests))
    .apply(new VariantStreamer(auth, ShardBoundary.Requirement.STRICT, VARIANT_FIELDS));
    
    PCollection<KV<Position, AlleleFreq>> refFreq = getFreq(variants, pipelineOptions.getMinFrequency());

    PCollection<KV<Position, ReadCounts>> readCountsTable =
        combineReads(reads, pipelineOptions.getSamplingFraction(), HASH_PREFIX, refFreq);
    
    // Converts our results to a single Map of Position keys to ReadCounts values.
    PCollectionView<Map<Position, ReadCounts>> view = readCountsTable
        .apply(View.<Position, ReadCounts>asMap());

    // Calculates the contamination estimate based on the resulting Map above.
    PCollection<String> result = p.begin().apply(Create.of(""))
        .apply(ParDo.of(new Maximizer(view)).withSideInputs(view));

    // Writes the result to the given output location in Cloud Storage.
    result.apply(TextIO.Write.to(pipelineOptions.getOutput()).named("WriteOutput").withoutSharding());

    p.run();

  }
  
  /**
   * Compute a PCollection of reference allele frequencies for SNPs of interest.
   * The SNPs all have only a single alternate allele, and neither the
   * reference nor the alternate allele have a population frequency < minFreq.
   * The results are returned in a PCollection indexed by Position.
   *
   * @param variants a set of variant calls for a reference population
   * @param minFreq the minimum allele frequency for the set
   * @return a PCollection mapping Position to AlleleCounts
   */
  static PCollection<KV<Position, AlleleFreq>> getFreq(
      PCollection<Variant> variants, double minFreq) {
    return variants.apply(Filter.byPredicate(VariantFunctions.IS_PASSING).named("PassingFilter"))
        .apply(Filter.byPredicate(VariantFunctions.IS_ON_CHROMOSOME).named("OnChromosomeFilter"))
        .apply(Filter.byPredicate(VariantFunctions.IS_NOT_LOW_QUALITY).named("NotLowQualityFilter"))
        .apply(Filter.byPredicate(VariantFunctions.IS_SINGLE_ALTERNATE_SNP).named("SNPFilter"))
        .apply(ParDo.of(new GetAlleleFreq()))
        .apply(Filter.byPredicate(new FilterFreq(minFreq)));
  }
  
  /**
   * Filter, pile up, and sample reads, then join against reference statistics.
   *
   * @param reads A PCollection of reads
   * @param samplingFraction Fraction of reads to keep
   * @param samplingPrefix A prefix used in generating hashes used in sampling
   * @param refCounts A PCollection mapping position to counts of alleles in
   *   a reference population.
   * @return A PCollection mapping Position to a ReadCounts proto
   */
  static PCollection<KV<Position, ReadCounts>> combineReads(PCollection<Read> reads,
      double samplingFraction, String samplingPrefix,
      PCollection<KV<Position, AlleleFreq>> refFreq) {
    // Runs filters on input Reads, splits into individual aligned bases (emitting the
    // base and quality) and grabs a sample of them based on a hash mod of Position.
    PCollection<KV<Position, ReadBaseQuality>> joinReadCounts =
        reads.apply(Filter.byPredicate(ReadFunctions.IS_ON_CHROMOSOME).named("IsOnChromosome"))
        .apply(Filter.byPredicate(ReadFunctions.IS_NOT_QC_FAILURE).named("IsNotQCFailure"))
        .apply(Filter.byPredicate(ReadFunctions.IS_NOT_DUPLICATE).named("IsNotDuplicate"))
        .apply(Filter.byPredicate(ReadFunctions.IS_PROPER_PLACEMENT).named("IsProperPlacement"))
        .apply(ParDo.of(new SplitReads()))
        .apply(Filter.byPredicate(new SampleReads(samplingFraction, samplingPrefix)));
    
    TupleTag<ReadBaseQuality> readCountsTag = new TupleTag<>();
    TupleTag<AlleleFreq> refFreqTag = new TupleTag<>();
    // Pile up read counts, then join against reference stats.
    PCollection<KV<Position, CoGbkResult>> joined = KeyedPCollectionTuple
        .of(readCountsTag, joinReadCounts)
        .and(refFreqTag, refFreq)
        .apply(CoGroupByKey.<Position>create());
    return joined.apply(ParDo.of(new PileupAndJoinReads(readCountsTag, refFreqTag)));
  }

  /**
   * Split reads into individual aligned bases and emit base + quality.
   */
  static class SplitReads extends DoFn<Read, KV<Position, ReadBaseQuality>> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
      List<ReadBaseWithReference> readBases = ReadFunctions.extractReadBases(c.element());
      if (!readBases.isEmpty()) {
        for (ReadBaseWithReference rb : readBases) {
          c.output(KV.of(rb.getRefPosition(), rb.getRbq()));
        }
      }
    }
  }

  /**
   * Sample bases via a hash mod of position.
   */
  static class SampleReads implements SerializableFunction<KV<Position, ReadBaseQuality>, Boolean> {

    private final double samplingFraction;
    private final String samplingPrefix;

    public SampleReads(double samplingFraction, String samplingPrefix) {
      this.samplingFraction = samplingFraction;
      this.samplingPrefix = samplingPrefix;
    }

    @Override
    public Boolean apply(KV<Position, ReadBaseQuality> input) {
      if (samplingFraction == 1.0) {
        return true;
      } else {
        byte[] msg;
        Position position = input.getKey();
        try {
          msg = (samplingPrefix + position.getReferenceName() + ":" + position.getPosition() + ":"
              + position.getReverseStrand()).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
          throw new AssertionError("UTF-8 not available - should not happen");
        }
        MessageDigest md;
        try {
          md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
          throw new AssertionError("MD5 not available - should not happen");
        }
        byte[] digest = md.digest(msg);
        if (digest.length != 16) {
          throw new AssertionError("MD5 should return 128 bits");
        }
        ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
        buffer.put(Arrays.copyOf(digest, Long.SIZE));
        return ((((double) buffer.getLong(0) / (double) ((long) 1 << 63)) + 1.0) * 0.5)
            < samplingFraction;
      }
    }
  }

  /**
   * Map a variant to a Position, AlleleFreq pair.
   */
  static class GetAlleleFreq extends DoFn<Variant, KV<Position, AlleleFreq>> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
      ListValue lv = c.element().getInfo().get("AF");
      if (lv != null && lv.getValuesCount() > 0) {
        Position position = Position.newBuilder()
            .setPosition(c.element().getStart())
            .setReferenceName(c.element().getReferenceName())
            .build();
        AlleleFreq af = new AlleleFreq();
        af.setRefFreq(lv.getValues(0).getNumberValue());
        af.setAltBases(c.element().getAlternateBasesList());
        af.setRefBases(c.element().getReferenceBases());
        c.output(KV.of(position, af));
      } else {
        // AF field wasn't populated in info, so we don't have frequency information
        // for this Variant.
        // TODO instead of straight throwing an exception, log a warning.  If at the end of this
        // step the number of AlleleFreqs retrieved is below a given threshold, then throw an
        // exception.
        throw new IllegalArgumentException("Variant " + c.element().getId() + " does not have "
            + "allele frequency information stored in INFO field AF.");
      }
    }
  }
  
  /**
   * Filters out AlleleFreqs for which the reference or alternate allele
   * frequencies are below a minimum specified at construction.
   */
  static class FilterFreq implements SerializableFunction<KV<Position, AlleleFreq>, Boolean> {

    private final double minFreq;
    
    public FilterFreq(double minFreq) {
      this.minFreq = minFreq;
    }
    
    @Override
    public Boolean apply(KV<Position, AlleleFreq> input) {
      double freq = input.getValue().getRefFreq();
      if (freq >= minFreq && (1.0 - freq) >= minFreq) {
        return true;
      }
      return false;
    }
  }

  /**
   * Piles up reads and joins them against reference population statistics.
   */
  static class PileupAndJoinReads
      extends DoFn<KV<Position, CoGbkResult>, KV<Position, ReadCounts>> {

    private final TupleTag<ReadBaseQuality> readCountsTag;
    private final TupleTag<AlleleFreq> refFreqTag;

    public PileupAndJoinReads(TupleTag<ReadBaseQuality> readCountsTag,
        TupleTag<AlleleFreq> refFreqTag) {
      this.readCountsTag = readCountsTag;
      this.refFreqTag = refFreqTag;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      AlleleFreq af = null;
      af = c.element().getValue().getOnly(refFreqTag, null);
      if (af == null) {
        // no ref stats
        return;
      }
      if (af.getAltBases().size() != 1) {
        throw new IllegalArgumentException("Wrong number (" + af.getAltBases().size() + ") of"
            + " alternate bases for Position " + c.element().getKey());
      }

      Iterable<ReadBaseQuality> reads = c.element().getValue().getAll(readCountsTag);

      ImmutableMultiset.Builder<ReadQualityCount> rqSetBuilder = ImmutableMultiset.builder();
      for (ReadBaseQuality r : reads) {
        ReadQualityCount.Base b;
        if (af.getRefBases().equals(r.getBase())) {
          b = ReadQualityCount.Base.REF;
        } else if (af.getAltBases().get(0).equals(r.getBase())) {
          b = ReadQualityCount.Base.NONREF;
        } else {
          b = ReadQualityCount.Base.OTHER;
        }
        ReadQualityCount rqc = new ReadQualityCount();
        rqc.setBase(b);
        rqc.setQuality(r.getQuality());
        rqSetBuilder.add(rqc);
      }

      ReadCounts rc = new ReadCounts();
      rc.setRefFreq(af.getRefFreq());
      for (Multiset.Entry<ReadQualityCount> entry : rqSetBuilder.build().entrySet()) {
        ReadQualityCount rq = entry.getElement();
        rq.setCount(entry.getCount());
        rc.addReadQualityCount(rq);
      }
      c.output(KV.of(c.element().getKey(), rc));
    }
  }

  /**
   * Calls the Solver to maximize via a univariate function the results of the pipeline, inputted
   * as a PCollectionView (the best way to retrieve our results as a Map in Dataflow).
   */
  static class Maximizer extends DoFn<Object, String> {

    private final PCollectionView<Map<Position, ReadCounts>> view;
    // Target absolute error for Brent's algorithm
    private static final double ABS_ERR = 0.00001;
    // Target relative error for Brent's algorithm
    private static final double REL_ERR = 0.0001;
    // Maximum number of evaluations of the Likelihood function in Brent's algorithm
    private static final int MAX_EVAL = 100;
    // Maximum number of iterations of Brent's algorithm
    private static final int MAX_ITER = 100;
    // Grid search step size
    private static final double GRID_STEP = 0.05;

    public Maximizer(PCollectionView<Map<Position, ReadCounts>> view) {
      this.view = view;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      c.output(Double.toString(Solver.maximize(new LikelihoodFn(c.sideInput(view)),
          0.0, 0.5, GRID_STEP, REL_ERR, ABS_ERR, MAX_ITER, MAX_EVAL)));
    }
  }
}
