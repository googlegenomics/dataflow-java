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
import com.google.api.services.genomics.Genomics.Annotationsets;
import com.google.api.services.genomics.model.Annotation;
import com.google.api.services.genomics.model.AnnotationSet;
import com.google.api.services.genomics.model.BatchCreateAnnotationsRequest;
import com.google.api.services.genomics.model.Position;
import com.google.cloud.genomics.dataflow.readers.bam.BAMShard;
import com.google.cloud.genomics.dataflow.readers.bam.ReadBAMTransform;
import com.google.cloud.genomics.dataflow.readers.bam.ReaderOptions;
import com.google.cloud.genomics.dataflow.readers.bam.ShardingPolicy;
import htsjdk.samtools.ValidationStringency;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.ApproximateQuantiles;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.model.PosRgsMq;
import com.google.cloud.genomics.dataflow.readers.ReadGroupStreamer;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.GenomicsUtils;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.RetryPolicy;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils.SexChromosomeFilter;
import com.google.common.collect.Lists;
import com.google.genomics.v1.CigarUnit;
import com.google.genomics.v1.Read;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;

/**
 * Class for calculating read depth coverage for a given data set.
 *
 * For each "bucket" in the given input references, this computes the average coverage (rounded to
 * six decimal places) across the bucket that 10%, 20%, 30%, etc. of the input ReadGroupsSets have
 * for each mapping quality of the reads (&lt;10:Low(L), 10-29:Medium(M), &gt;=30:High(H)) as well as
 * these same percentiles of ReadGroupSets for all reads regardless of mapping quality (Mapping
 * quality All(A)).
 *
 * There is also the option to change the number of quantiles accordingly (numQuantiles = 5 would
 * give you the minimum ReadGroupSet mean coverage for each and across all mapping qualities, the
 * 25th, 50th, and 75th percentiles, and the maximum of these values).
 *
 * See
 * http://googlegenomics.readthedocs.org/en/latest/use_cases/analyze_reads/calculate_coverage.html
 * for running instructions.
 */
public class CalculateCoverage {

  /**
   * Options required to run this pipeline.
   *
   * When specifying references, if you specify a nonzero starting point for any given reference,
   * reads that start before that reference, even if they overlap with the reference, will NOT be
   * counted.
   */
  public static interface Options extends ShardOptions {

    @Description("Cloud storage prefix containing BAM and BAI files from which to read or a path "
        + "to a local file containing the newline-separated prefixes."
        + " Use one of --bamInput, --inputDatasetId, or --readGroupSetIds.")
    @Default.String("")
    String getBamInput();
    void setBamInput(String value);

    @Description("A comma delimited list of the IDs of the Google Genomics ReadGroupSets this "
        + "pipeline is working with. Default (empty) indicates all ReadGroupSets in InputDatasetId."
        + "  This or InputDatasetId must be set.  InputDatasetId overrides ReadGroupSetIds "
        + "(if InputDatasetId is set, this field will be ignored).  All of the referenceSetIds for"
        + " all ReadGroupSets in this list must be the same for the purposes of setting the"
        + " referenceSetId of the output AnnotationSet."
        + " Use one of --bamInput, --inputDatasetId, or --readGroupSetIds.")
    @Default.String("")
    String getReadGroupSetIds();

    void setReadGroupSetIds(String readGroupSetId);

    @Description("The ID of the Google Genomics Dataset that the pipeline will get its input reads"
        + " from.  Default (empty) means to use ReadGroupSetIds instead.  This or ReadGroupSetIds"
        + " must be set.  InputDatasetId overrides ReadGroupSetIds (if this field is set, "
        + "ReadGroupSetIds will be ignored).  All of the referenceSetIds for all ReadGroupSets in"
        + " this Dataset must be the same for the purposes of setting the referenceSetId"
        + " of the output AnnotationSet."
        + " Use one of --bamInput, --inputDatasetId, or --readGroupSetIds.")
    @Default.String("")
    String getInputDatasetId();

    void setInputDatasetId(String inputDatasetId);

    @Validation.Required
    @Description("The ID of the Google Genomics Dataset that the output AnnotationSet will be "
        + "posted to.")
    @Default.String("")
    String getOutputDatasetId();

    void setOutputDatasetId(String outputDatasetId);

    @Description("The Google Genomics reference set id to use for the annotation set."
    + "Only used when --bamInput is used. Defaults to \"EMWV_ZfLxrDY-wE\" for hg19.")
    @Default.String("EMWV_ZfLxrDY-wE")
    String getReferenceSetId();
    void setReferenceSetId(String referenceSetId);

    @Description("For BAM file input, the maximum size in bytes of a shard processed by a "
        + "single worker. Only used when --bamInput is used. (For --inputDatasetId or "
        + "--readGroupSetIds, shard size is controlled by --basesPerShard.)")
    @Default.Integer(100 * 1024 * 1024)  // 100 MB
    int getMaxShardSizeBytes();
    void setMaxShardSizeBytes(int maxShardSizeBytes);

    @Description("The bucket width you would like to calculate coverage for.  Default is 2048.  "
        + "Buckets are non-overlapping.  If bucket width does not divide into the reference "
        + "size, then the remainder will be a smaller bucket at the end of the reference.")
    @Default.Integer(2048)
    int getBucketWidth();

    void setBucketWidth(int bucketWidth);

    @Description("The number of quantiles you would like to calculate for in the output.  Default"
        + "is 11 (minimum value for a particular grouping, 10-90 percentiles, and the maximum"
        + " value).  The InputDataset or list of ReadGroupSetIds specified must have an amount of "
        + "ReadGroupSetIds greater than or equal to the number of quantiles that you are asking "
        + "for.")
    @Default.Integer(11)
    int getNumQuantiles();

    void setNumQuantiles(int numQuantiles);

    @Description("This provides the name for the AnnotationSet. Default (empty) will set the "
        + "name to the input References. For more information on AnnotationSets, please visit: "
        + "https://cloud.google.com/genomics/v1beta2/reference/annotationSets#resource")
    @Default.String("")
    String getAnnotationSetName();

    void setAnnotationSetName(String name);
  }

  // Tip: Use the API explorer to test which fields to include in partial responses.
  // https://developers.google.com/apis-explorer/#p/genomics/v1/genomics.reads.stream?fields=alignments(alignedSequence%252Cid)&_h=2&resource=%257B%250A++%2522readGroupSetId%2522%253A+%2522CMvnhpKTFhD3he72j4KZuyc%2522%252C%250A++%2522referenceName%2522%253A+%2522chr17%2522%252C%250A++%2522start%2522%253A+%252241196311%2522%252C%250A++%2522end%2522%253A+%252241196312%2522%250A%257D&
  private static final String READ_FIELDS = "alignments(alignment,readGroupSetId)";
  private static Options options;
  private static Pipeline p;
  private static OfflineAuth auth;

  public static void registerPipelineCoders(Pipeline p) {
    CoderRegistry cr = p.getCoderRegistry();
    cr.registerCoderForClass(Annotation.class,
      (Coder<Annotation>) GenericJsonCoder.of(Annotation.class));
    cr.registerCoderForClass(AnnotationSet.class,
      (Coder<AnnotationSet>) GenericJsonCoder.of(AnnotationSet.class));
    cr.registerCoderForClass(BatchCreateAnnotationsRequest.class,
      (Coder<BatchCreateAnnotationsRequest>) GenericJsonCoder
        .of(BatchCreateAnnotationsRequest.class));
    cr.registerCoderForClass(PosRgsMq.class,
      (Coder<PosRgsMq>) GenericJsonCoder.of(PosRgsMq.class));
    cr.registerCoderForClass(Position.class,
      (Coder<Position>) GenericJsonCoder.of(Position.class));
  }

  public static void main(String[] args) throws GeneralSecurityException, IOException, URISyntaxException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(Options.class);
    options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    auth = GenomicsOptions.Methods.getGenomicsAuth(options);

    p = Pipeline.create(options);
    registerPipelineCoders(p);

    if (options.getBamInput().isEmpty() && options.getInputDatasetId().isEmpty() && options.getReadGroupSetIds().isEmpty()) {
      throw new IllegalArgumentException("BamInput or InputDatasetId or ReadGroupSetIds must be specified");
    }

    PCollection<Read> reads = null;
    String referenceSetId = options.getReferenceSetId();

    if (!options.getBamInput().isEmpty()) {

      if (options.isAllReferences()) {
        throw new IllegalArgumentException("--allReferences is not currently supported for BAM "
        + "file reading. Instead use --references and list all of the genomic regions in the file.");
      }

      final List<Contig> contigs = Lists.newArrayList(Contig.parseContigsFromCommandLine(options.getReferences()));
      final ReaderOptions readerOptions = new ReaderOptions(
          ValidationStringency.LENIENT,
          false);  // Do not include unmapped reads.

      ShardingPolicy policy = new ShardingPolicy() {
        final int MAX_BYTES_PER_SHARD = options.getMaxShardSizeBytes();
        @Override
        public Boolean apply(BAMShard shard) {
          return shard.approximateSizeInBytes() > MAX_BYTES_PER_SHARD;
        }
      };

      reads = ReadBAMTransform.getReadsFromBAMFilesSharded (
          p,
          options,
          auth,
          contigs,
          readerOptions,
          options.getBamInput(),
          policy);

    } else {

      List<String> rgsIds;
      if (options.getInputDatasetId().isEmpty()) {
        rgsIds = Lists.newArrayList(options.getReadGroupSetIds().split(","));
      } else {
        rgsIds = GenomicsUtils.getReadGroupSetIds(options.getInputDatasetId(), auth);
      }

      if (rgsIds.size() < options.getNumQuantiles()) {
        throw new IllegalArgumentException("Number of ReadGroupSets must be greater than or equal to"
            + " the number of requested quantiles.");
      }

      // Grab one ReferenceSetId to be used within the pipeline to confirm that all ReadGroupSets
      // are associated with the same ReferenceSet.
      referenceSetId = GenomicsUtils.getReferenceSetId(rgsIds.get(0), auth);
      if (Strings.isNullOrEmpty(referenceSetId)) {
        throw new IllegalArgumentException("No ReferenceSetId associated with ReadGroupSetId "
            + rgsIds.get(0)
            + ". All ReadGroupSets in given input must have an associated ReferenceSet.");
      }

      reads = p.begin()
          .apply(Create.of(rgsIds))
          .apply(ParDo.of(new CheckMatchingReferenceSet(referenceSetId, auth)))
          .apply(new ReadGroupStreamer(auth, ShardBoundary.Requirement.STRICT, READ_FIELDS, SexChromosomeFilter.INCLUDE_XY));
    }

    // Create our destination AnnotationSet for the associated ReferenceSet.
    AnnotationSet annotationSet = createAnnotationSet(referenceSetId);

    PCollection<KV<PosRgsMq, Double>> coverageMeans = reads.apply("CalculateCoverateMean",
        new CalculateCoverageMean(options.getBucketWidth()));
    PCollection<KV<Position, KV<PosRgsMq.MappingQuality, List<Double>>>> quantiles
        = coverageMeans.apply("CalculateQuantiles", new CalculateQuantiles(options.getNumQuantiles()));
    PCollection<KV<Position, Iterable<KV<PosRgsMq.MappingQuality, List<Double>>>>> answer =
        quantiles.apply(GroupByKey.<Position, KV<PosRgsMq.MappingQuality, List<Double>>>create());
    answer.apply("CreateAnnotations", ParDo.of(new CreateAnnotations(annotationSet.getId(), auth, true)));

    p.run();
  }

  static class CheckMatchingReferenceSet extends DoFn<String, String> {
    private final String referenceSetIdForAllReadGroupSets;
    private final OfflineAuth auth;

    public CheckMatchingReferenceSet(String referenceSetIdForAllReadGroupSets,
        OfflineAuth auth) {
      this.referenceSetIdForAllReadGroupSets = referenceSetIdForAllReadGroupSets;
      this.auth = auth;
    }

    @ProcessElement
    public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
      String readGroupSetId = c.element();
      String referenceSetId = GenomicsUtils.getReferenceSetId(readGroupSetId, auth);
      if (Strings.isNullOrEmpty(referenceSetId)) {
        throw new IllegalArgumentException("No ReferenceSetId associated with ReadGroupSetId "
            + readGroupSetId
            + ". All ReadGroupSets in given input must have an associated ReferenceSet.");
      }
      if (!referenceSetIdForAllReadGroupSets.equals(referenceSetId)) {
        throw new IllegalArgumentException("ReadGroupSets in given input must all be associated with"
            + " the same ReferenceSetId : " + referenceSetId);
      }
      c.output(readGroupSetId);
    }
  }

  /**
   * Composite PTransform that takes in Reads read in from various ReadGroupSetIds using the
   * Genomics API and calculates the coverage mean at every distinct PosRgsMq object for the data
   * input (Position, ReadgroupSetId, and Mapping Quality).
   */
  public static class CalculateCoverageMean extends PTransform<PCollection<Read>,
      PCollection<KV<PosRgsMq, Double>>> {
    private final long bucketWidth;

    public CalculateCoverageMean(long bucketWidth) {
      this.bucketWidth = bucketWidth;
    }

    @Override
    public PCollection<KV<PosRgsMq, Double>> expand(PCollection<Read> input) {
      return input.apply(ParDo.of(new CoverageCounts(bucketWidth)))
          .apply(Combine.<PosRgsMq, Long>perKey(new SumCounts()))
          .apply(ParDo.of(new CoverageMeans()));
    }
  }

  static class CoverageCounts extends DoFn<Read, KV<PosRgsMq, Long>> {

    private static final int LOW_MQ = 10;
    private static final int HIGH_MQ = 30;
    private final long bucketWidth;

    public CoverageCounts(long bucketWidth) {
      this.bucketWidth = bucketWidth;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element().getAlignment() != null) { //is mapped
        // Calculate length of read
        long readLength = 0;
        for (CigarUnit cigar : c.element().getAlignment().getCigarList()) {
          switch (cigar.getOperation()) {
            case ALIGNMENT_MATCH:
            case SEQUENCE_MATCH:
            case SEQUENCE_MISMATCH:
            case PAD:
            case DELETE:
            case SKIP:
              readLength += cigar.getOperationLength();
              break;
          }
        }
        // Calculate readEnd by shifting readStart by readLength
        long readStart = c.element().getAlignment().getPosition().getPosition();
        long readEnd = readStart + readLength;
        // Calculate the index of the first bucket this read falls into
        long bucket = readStart / bucketWidth * bucketWidth;
        long readCurr = readStart;
        while (readCurr < readEnd) {
          // Loop over read to generate output for each bucket
          long baseCount = 0;
          long dist = Math.min(bucket + bucketWidth, readEnd) - readCurr;
          readCurr += dist;
          baseCount += dist;
          Position position = new Position()
              .setPosition(bucket)
              .setReferenceName(c.element().getAlignment().getPosition().getReferenceName());
          Integer mq = c.element().getAlignment().getMappingQuality();
          if (mq == null) {
            mq = 0;
          }
          PosRgsMq.MappingQuality mqEnum;
          if (mq < LOW_MQ) {
            mqEnum = PosRgsMq.MappingQuality.L;
          } else if (mq >= LOW_MQ && mq < HIGH_MQ) {
            mqEnum = PosRgsMq.MappingQuality.M;
          } else {
            mqEnum = PosRgsMq.MappingQuality.H;
          }
          c.output(KV.of(new PosRgsMq(position, c.element().getReadGroupSetId(), mqEnum), baseCount));
          c.output(KV.of(new PosRgsMq(
              position, c.element().getReadGroupSetId(), PosRgsMq.MappingQuality.A), baseCount));
          bucket += bucketWidth;
        }
      }
    }
  }

  static class SumCounts implements SerializableFunction<Iterable<Long>, Long> {

    @Override
    public Long apply(Iterable<Long> input) {
      long ans = 0;
      for (Long l : input) {
        ans += l;
      }
      return ans;
    }
  }

  static class CoverageMeans extends DoFn<KV<PosRgsMq, Long>, KV<PosRgsMq, Double>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV ans = KV.of(c.element().getKey(),
          (double) c.element().getValue()
          / (double) c.getPipelineOptions().as(Options.class).getBucketWidth());
      c.output(ans);
    }
  }

  /**
   * Composite PTransform that takes in the KV representing the coverage mean for a distinct
   * PosRgsMq object (Position, ReadgroupsetId, and Mapping Quality) and calculates the quantiles
   * such that 10% of the ReadGroupSetIds at a given Position (bucket) and mapping quality
   * have at least this much coverage, and the same for 20%, etc., all the way up until 90%. It puts
   * this into a KV object that can later be used to create annotations of the results.
   *
   * The percentiles will change based on the number of quantiles specified (see class description).
   */
  public static class CalculateQuantiles extends PTransform<PCollection<KV<PosRgsMq, Double>>,
      PCollection<KV<Position, KV<PosRgsMq.MappingQuality, List<Double>>>>> {

    private final int numQuantiles;

    public CalculateQuantiles(int numQuantiles) {
      this.numQuantiles = numQuantiles;
    }

    @Override
    public PCollection<KV<Position, KV<PosRgsMq.MappingQuality, List<Double>>>> expand(
        PCollection<KV<PosRgsMq, Double>> input) {
      return input.apply(ParDo.of(new RemoveRgsId()))
          .apply(ApproximateQuantiles
              .<KV<Position, PosRgsMq.MappingQuality>, Double>perKey(numQuantiles))
          .apply(ParDo.of(new MoveMapping()));
    }
  }

  static class RemoveRgsId extends DoFn<KV<PosRgsMq, Double>,
      KV<KV<Position, PosRgsMq.MappingQuality>, Double>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV ans = KV.of(KV.of(c.element().getKey().getPos(), c.element().getKey().getMq()),
          c.element().getValue());
      c.output(ans);
    }
  }

  static class MoveMapping extends DoFn<KV<KV<Position, PosRgsMq.MappingQuality>, List<Double>>,
      KV<Position, KV<PosRgsMq.MappingQuality, List<Double>>>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV ans = KV.of(c.element().getKey().getKey(),
          KV.of(c.element().getKey().getValue(), c.element().getValue()));
      c.output(ans);
    }
  }

  /*
  TODO: If a particular batchCreateAnnotations request fails more than 4 times and the bundle gets
  retried by the dataflow pipeline, some annotations may be written multiple times.
  */
  static class CreateAnnotations extends
      DoFn<KV<Position, Iterable<KV<PosRgsMq.MappingQuality, List<Double>>>>, Annotation> {

    private final String asId;
    private final OfflineAuth auth;
    private final List<Annotation> currAnnotations;
    private final boolean write;

    public CreateAnnotations(String asId, OfflineAuth auth, boolean write) {
      this.asId = asId;
      this.auth = auth;
      this.currAnnotations = Lists.newArrayList();
      this.write = write;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws GeneralSecurityException, IOException {
      Options pOptions = c.getPipelineOptions().as(Options.class);
      Position bucket = c.element().getKey();
      Annotation a = new Annotation()
          .setAnnotationSetId(asId)
          .setStart(bucket.getPosition())
          .setEnd(bucket.getPosition() + pOptions.getBucketWidth())
          .setReferenceName(bucket.getReferenceName())
          .setType("GENERIC")
          .setInfo(new HashMap<String, List<Object>>());
      for (KV<PosRgsMq.MappingQuality, List<Double>> mappingQualityKV : c.element().getValue()) {
        List<Object> output = Lists.newArrayList();
        for (int i = 0; i < mappingQualityKV.getValue().size(); i++) {
          double value = Math.round(mappingQualityKV.getValue().get(i) * 1000000.0) / 1000000.0;
          output.add(Double.toString(value));
        }
        a.getInfo().put(mappingQualityKV.getKey().toString(), output);
      }
      if (write) {
        currAnnotations.add(a);
        if (currAnnotations.size() == 512) {
          // Batch create annotations once we hit the max amount for a batch
          batchCreateAnnotations();
        }
      }
      c.output(a);
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) throws IOException, GeneralSecurityException {
      // Finish up any leftover annotations at the end.
      if (write && !currAnnotations.isEmpty()) {
        batchCreateAnnotations();
      }
    }

    private void batchCreateAnnotations() throws IOException, GeneralSecurityException {
      Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);
      Genomics.Annotations.BatchCreate aRequest = genomics.annotations().batchCreate(
              new BatchCreateAnnotationsRequest().setAnnotations(currAnnotations));
      RetryPolicy retryP = RetryPolicy.nAttempts(4);
      retryP.execute(aRequest);
      currAnnotations.clear();
    }
  }

  private static AnnotationSet createAnnotationSet(String referenceSetId)
      throws GeneralSecurityException, IOException {
    if (referenceSetId == null) {
      throw new IOException("ReferenceSetId was null for this readgroupset");
    }
    // Create a new annotation set using the given datasetId, or the one that this data was from if
    // one was not provided
    AnnotationSet as = new AnnotationSet();
    as.setDatasetId(options.getOutputDatasetId());

    if (!"".equals(options.getAnnotationSetName())) {
      as.setName(options.getAnnotationSetName());
    } else {
      if (!options.isAllReferences()) {
        as.setName("Read Depth for " + options.getReferences());
      } else {
        as.setName("Read Depth for all references");
      }
    }
    as.setReferenceSetId(referenceSetId);
    as.setType("GENERIC");
    Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);
    Annotationsets.Create asRequest = genomics.annotationsets().create(as);
    AnnotationSet asWithId = asRequest.execute();
    return asWithId;
  }
}
