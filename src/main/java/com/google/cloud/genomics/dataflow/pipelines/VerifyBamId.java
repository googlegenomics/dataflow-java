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

import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.Position;
import com.google.api.services.genomics.model.ReadGroupSet;
import com.google.api.services.genomics.model.Reference;
import com.google.api.services.genomics.model.SearchReferencesRequest;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
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
import com.google.cloud.genomics.dataflow.functions.LikelihoodFn;
import com.google.cloud.genomics.dataflow.model.AlleleFreq;
import com.google.cloud.genomics.dataflow.model.ReadBaseQuality;
import com.google.cloud.genomics.dataflow.model.ReadBaseWithReference;
import com.google.cloud.genomics.dataflow.model.ReadCounts;
import com.google.cloud.genomics.dataflow.model.ReadQualityCount;
import com.google.cloud.genomics.dataflow.readers.ReadStreamer;
import com.google.cloud.genomics.dataflow.readers.VariantStreamer;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ReadUtils;
import com.google.cloud.genomics.dataflow.utils.Solver;
import com.google.cloud.genomics.dataflow.utils.VariantUtils;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.genomics.v1.Read;
import com.google.genomics.v1.StreamReadsRequest;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.Variant;
import com.google.protobuf.ListValue;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test a set of reads for contamination.
 *
 * Takes a set of specified ReadGroupSets of reads to test and statistics on reference allele
 * frequencies for SNPs with a single alternative from a specified set of VariantSets.
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

  private static VerifyBamId.VerifyBamIdOptions options;
  private static Pipeline p;
  private static GenomicsFactory.OfflineAuth auth;

  /**
   * Constant that represents the size that user given references will be parsed into for each
   * individual request.
   */
  private static final long SHARD_SIZE = 10000000L;
  
  /**
   * String prefix used for sampling hash function
   */
  private static final String HASH_PREFIX = "";
  
  /**
   * Options required to run this pipeline.
   */
  public static interface VerifyBamIdOptions extends GenomicsDatasetOptions, GCSOptions {

    @Description("A comma delimited list of the IDs of the Google Genomics ReadGroupSets this "
        + "pipeline is working with. Default (empty) indicates all ReadGroupSets in InputDatasetId."
        + "  This(and variantSetIds) or InputDatasetId must be set.  InputDatasetId overrides "
        + "ReadGroupSetIds (if InputDatasetId is set, this field will be ignored).")
    @Default.String("")
    String getReadGroupSetIds();

    void setReadGroupSetIds(String readGroupSetId);

    @Description("A comma delimited list of the IDs of the Google Genomics VariantSets this "
        + "pipeline is working with. Default (empty) indicates all VariantSets in InputDatasetId."
        + "  This(and readGroupSetIds) or InputDatasetId must be set.  InputDatasetId overrides "
        + "VariantSetIds (if InputDatasetId is set, this field will be ignored).")
    @Default.String("")
    String getVariantSetIds();

    void setVariantSetIds(String variantSetId);

    @Description("The ID of the Google Genomics Dataset that the pipeline will get its input reads"
        + " from.  Default (empty) means to use ReadGroupSetIds and VariantSetIds instead.  This or"
        + " ReadGroupSetIds and VariantSetIds must be set.  InputDatasetId overrides"
        + " ReadGroupSetIds and VariantSetIds (if this field is set, ReadGroupSetIds and"
        + " VariantSetIds will be ignored).")
    @Default.String("")
    String getInputDatasetId();

    void setInputDatasetId(String inputDatasetId);

    @Description("The minimum allele frequency to use in analysis.  Defaults to 0.01.")
    @Default.Double(0.01)
    double getMinFrequency();

    void setMinFrequency(double minFrequency);

    @Description("The fraction of positions to check.  Defaults to 0.01.")
    @Default.Double(0.01)
    double getSamplingFraction();

    void setSamplingFraction(double minFrequency);
  }

  /**
   * Run the VerifyBamId algorithm and output the resulting contamination estimate.
   */
  public static void main(String[] args) throws GeneralSecurityException, IOException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(VerifyBamIdOptions.class);
    options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(VerifyBamId.VerifyBamIdOptions.class);
    // Option validation is not yet automatic, we make an explicit call here.
    GenomicsDatasetOptions.Methods.validateOptions(options);
    auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    
    p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);
    DataflowWorkarounds.registerCoder(p, Read.class, SerializableCoder.of(Read.class));
    DataflowWorkarounds.registerCoder(p, Variant.class, SerializableCoder.of(Variant.class));
    DataflowWorkarounds.registerCoder(p, ReadBaseQuality.class,
        GenericJsonCoder.of(ReadBaseQuality.class));
    DataflowWorkarounds.registerCoder(p, AlleleFreq.class, GenericJsonCoder.of(AlleleFreq.class));
    DataflowWorkarounds.registerCoder(p, ReadCounts.class, GenericJsonCoder.of(ReadCounts.class));

    if (options.getInputDatasetId().isEmpty()
        && (options.getReadGroupSetIds().isEmpty() || options.getVariantSetIds().isEmpty())) {
      throw new IllegalArgumentException("InputDatasetId or ReadGroupSetIds and VariantSetIds must"
          + " be specified");
    }

    List<String> rgsIds;
    List<String> vsIds;
    if (options.getInputDatasetId().isEmpty()) {
      rgsIds = Lists.newArrayList(options.getReadGroupSetIds().split(","));
      vsIds = Lists.newArrayList(options.getVariantSetIds().split(","));
    } else {
      rgsIds = ReadStreamer.getReadGroupSetIds(options.getInputDatasetId(), auth);
      vsIds = VariantStreamer.getVariantSetIds(options.getInputDatasetId(), auth);
    }
    
    List<Contig> contigs;
    String referenceSetId = checkReferenceSetIds(rgsIds);
    if (options.isAllReferences()) {
      contigs = getAllReferences(referenceSetId);
    } else {
      contigs = parseReferences(options.getReferences(), referenceSetId);
    }

    /*
    TODO:  We can reduce the number of requests needed to be created by doing the following:
    1. Stream the Variants first (rather than concurrently with the Reads).  Select a subset of
       them equal to some threshold (say 50K by default).
    2. Create the requests for streaming Reads by running a ParDo over the selected Variants
       to get their ranges (we only need to stream Reads that overlap the selected Variants).
    3. Stream the Reads from the created requests.
    */
      
    // Reads in Reads.
    PCollection<Read> reads = getReadsFromAPI(rgsIds);

    // Reads in Variants.  TODO potentially provide an option to load the Variants from a file.
    PCollection<Variant> variants = getVariantsFromAPI(vsIds);
    
    PCollection<KV<Position, AlleleFreq>> refFreq = getFreq(variants, options.getMinFrequency());

    PCollection<KV<Position, ReadCounts>> readCountsTable =
        combineReads(reads, options.getSamplingFraction(), HASH_PREFIX, refFreq);
    
    // Converts our results to a single Map of Position keys to ReadCounts values.
    PCollectionView<Map<Position, ReadCounts>> view = readCountsTable
        .apply(View.<Position, ReadCounts>asMap().withSingletonValues());

    // Calculates the contamination estimate based on the resulting Map above.
    PCollection<String> result = p.begin().apply(Create.of(""))
        .apply(ParDo.of(new Maximizer(view)).withSideInputs(view));

    // Writes the result to the given output location in Cloud Storage.
    result.apply(TextIO.Write.to(options.getOutput()).named("WriteOutput").withoutSharding());

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
    return variants.apply(Filter.by(VariantUtils.IS_PASSING).named("PassingFilter"))
        .apply(Filter.by(VariantUtils.IS_ON_CHROMOSOME).named("OnChromosomeFilter"))
        .apply(Filter.by(VariantUtils.IS_NOT_LOW_QUALITY).named("NotLowQualityFilter"))
        .apply(Filter.by(VariantUtils.IS_SINGLE_ALTERNATE_SNP).named("SNPFilter"))
        .apply(ParDo.of(new GetAlleleFreq()))
        .apply(Filter.by(new FilterFreq(minFreq)));
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
        reads.apply(Filter.by(ReadUtils.IS_ON_CHROMOSOME).named("IsOnChromosome"))
        .apply(Filter.by(ReadUtils.IS_NOT_QC_FAILURE).named("IsNotQCFailure"))
        .apply(Filter.by(ReadUtils.IS_NOT_DUPLICATE).named("IsNotDuplicate"))
        .apply(Filter.by(ReadUtils.IS_PROPER_PLACEMENT).named("IsProperPlacement"))
        .apply(ParDo.of(new SplitReads()))
        .apply(Filter.by(new SampleReads(samplingFraction, samplingPrefix)));
    
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
      List<ReadBaseWithReference> readBases = ReadUtils.extractReadBases(c.element());
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
        Position p = input.getKey();
        try {
          msg = (samplingPrefix + p.getReferenceName() + ":" + p.getPosition() + ":"
              + p.getReverseStrand()).getBytes("UTF-8");
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
        Position p = new Position()
            .setPosition(c.element().getStart())
            .setReferenceName(c.element().getReferenceName());
        AlleleFreq af = new AlleleFreq();
        af.setRefFreq(lv.getValues(0).getNumberValue());
        af.setAltBases(c.element().getAlternateBasesList());
        af.setRefBases(c.element().getReferenceBases());
        c.output(KV.of(p, af));
      } else {
        // AF field wasn't populated in info, so we don't have frequency information
        // for this Variant.
        // TODO instead of straight throwing an exception, log a warning.  If at the end of this
        // step the number of AlleleFreqs retrieved is below a given threshold, then throw an
        // exception.
        throw new IllegalArgumentException("Variant " + c.element().getId() + " does not have "
            + "allele frequency information stored.");
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
  
  /**
   * Checks to make sure all of the input ReadGroupSets have the same referenceSetId.
   * 
   * @param readGroupSetIds list of input ReadGroupSet ids to check
   * @return the referenceSetId of the given ReadGroupSets
   */
  private static String checkReferenceSetIds(List<String> readGroupSetIds)
      throws GeneralSecurityException, IOException {
    String referenceSetId = null;
    for (String rgsId : readGroupSetIds) {
      Genomics.Readgroupsets.Get rgsRequest = auth.getGenomics(auth.getDefaultFactory())
          .readgroupsets().get(rgsId).setFields("referenceSetId");
      ReadGroupSet rgs = rgsRequest.execute();
      if (referenceSetId == null) {
        referenceSetId = rgs.getReferenceSetId();
      } else if (!rgs.getReferenceSetId().equals(referenceSetId)) {
        throw new IllegalArgumentException("ReferenceSetIds must be the same for all"
            + " ReadGroupSets in given input.");
      }
    }
    return referenceSetId;
  }
  
  /**
   * Parses and shards all of the References in the given ReferenceSet.
   * 
   * @param referenceSetId the id of the ReferenceSet we want the Contigs for
   * @return list of Contigs representing all of the References in the given ReferenceSet
   */
  public static List<Contig> getAllReferences(String referenceSetId)
      throws IOException, GeneralSecurityException {
    List<Contig> contigs = Lists.newArrayList();
    Genomics.References.Search refRequest = auth.getGenomics(auth.getDefaultFactory())
        .references()
        .search(new SearchReferencesRequest().setReferenceSetId(referenceSetId));
    List<Reference> referencesList = refRequest.execute().getReferences();
    for (Reference r : referencesList) {
      contigs.add(new Contig(r.getName(), 0L, r.getLength()));
    }
    contigs = Lists.newArrayList(FluentIterable.from(contigs)
        .transformAndConcat(new Function<Contig, Iterable<Contig>>() {
          @Override
          public Iterable<Contig> apply(Contig contig) {
            return contig.getShards(SHARD_SIZE);
          }
        }));
    Collections.shuffle(contigs);
    return contigs;
  }
  
  /**
   * Parses and shards the References in the given ReferenceSet.
   * 
   * @param references the user given references from the command line.
   * @param referenceSetId the id of the ReferenceSet we want the Contigs for
   * @return list of Contigs representing all of the References in the given ReferenceSet
   */
  public static List<Contig> parseReferences(String references,
      String referenceSetId) throws IOException, GeneralSecurityException {
    List<String> splitReferences = Lists.newArrayList(references.split(","));
    List<Contig> contigs = Lists.newArrayList();
    List<Reference> referencesList = null; // If needed
    for (String ref : splitReferences) {
      String[] splitPieces = ref.split(":");
      if (splitPieces.length != 3 && referencesList == null) {
        // referencesList hasn't been needed up until this point, so we must request it.
        // Assume that they are asking for one entire specific reference.
        Genomics.References.Search refRequest = auth.getGenomics(auth.getDefaultFactory())
            .references()
            .search(new SearchReferencesRequest().setReferenceSetId(referenceSetId));
        referencesList = refRequest.execute().getReferences();
        for (Reference r : referencesList) {
          if (r.getName().equals(splitPieces[0])) { // Found the reference we want.
            contigs.add(new Contig(splitPieces[0], 0L, r.getLength()));
            break;
          }
        }
      } else if (splitPieces.length != 3) {
        // Assume that they are asking for one entire specific reference.
        for (Reference r : referencesList) {
          if (r.getName().equals(splitPieces[0])) { // Found the reference we want.
            contigs.add(new Contig(splitPieces[0], 0L, r.getLength()));
            break;
          }
        }
      } else {
        contigs.add(
            new Contig(splitPieces[0], Long.valueOf(splitPieces[1]), Long.valueOf(splitPieces[2])));
      }
    }
    contigs = Lists.newArrayList(FluentIterable.from(contigs)
        .transformAndConcat(new Function<Contig, Iterable<Contig>>() {
          @Override
          public Iterable<Contig> apply(Contig contig) {
            return contig.getShards(SHARD_SIZE);
          }
        }));
    Collections.shuffle(contigs);
    return contigs;
  }
  
  private static PCollection<Read> getReadsFromAPI(List<String> rgsIds)
      throws IOException, GeneralSecurityException {
    List<StreamReadsRequest> requests = Lists.newArrayList();
    for (String r : rgsIds) {
      if (options.isAllReferences()) {
        requests.add(ReadStreamer.getReadRequests(r));
      } else {
        requests.addAll(ReadStreamer.getReadRequests(r, options.getReferences()));
      }
    }
    PCollection<StreamReadsRequest> readRequests = p.begin().apply(Create.of(requests));
    return readRequests.apply(new ReadStreamer.StreamReads());
  }
  
  private static PCollection<Variant> getVariantsFromAPI(List<String> vsIds)
      throws IOException, GeneralSecurityException {
    List<StreamVariantsRequest> requests = Lists.newArrayList();
    for (String v : vsIds) {
      if (options.isAllReferences()) {
        requests.add(VariantStreamer.getVariantRequests(v));
      } else {
        requests.addAll(VariantStreamer.getVariantRequests(v, options.getReferences()));
      }
    }
    PCollection<StreamVariantsRequest> variantRequests = p.begin().apply(Create.of(requests));
    return variantRequests.apply(new VariantStreamer.StreamVariants());
  }
}