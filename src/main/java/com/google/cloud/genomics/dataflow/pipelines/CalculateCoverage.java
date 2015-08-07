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
import com.google.api.services.genomics.model.Annotation;
import com.google.api.services.genomics.model.AnnotationSet;
import com.google.api.services.genomics.model.BatchCreateAnnotationsRequest;
import com.google.api.services.genomics.model.Position;
import com.google.api.services.genomics.model.RangePosition;
import com.google.api.services.genomics.model.ReadGroupSet;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Proto2Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.ApproximateQuantiles;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.model.PosRgsMq;
import com.google.cloud.genomics.dataflow.readers.ReadStreamer;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.GenomicsUtils;
import com.google.cloud.genomics.utils.RetryPolicy;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.common.collect.Lists;
import com.google.genomics.v1.CigarUnit;
import com.google.genomics.v1.Read;
import com.google.genomics.v1.StreamReadsRequest;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

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

  private static CoverageOptions options;
  private static Pipeline p;
  private static GenomicsFactory.OfflineAuth auth;
  private static final Logger LOG = Logger.getLogger(CalculateCoverage.class.getName());

  /**
   * Options required to run this pipeline.
   *
   * When specifying references, if you specify a nonzero starting point for any given reference,
   * reads that start before that reference, even if they overlap with the reference, will NOT be
   * counted.
   */
  public static interface CoverageOptions extends GenomicsDatasetOptions, GCSOptions {

    @Description("A comma delimited list of the IDs of the Google Genomics ReadGroupSets this "
        + "pipeline is working with. Default (empty) indicates all ReadGroupSets in InputDatasetId."
        + "  This or InputDatasetId must be set.  InputDatasetId overrides ReadGroupSetIds "
        + "(if InputDatasetId is set, this field will be ignored).  All of the referenceSetIds for"
        + " all ReadGroupSets in this list must be the same for the purposes of setting the"
        + " referenceSetId of the output AnnotationSet.")
    @Default.String("")
    String getReadGroupSetIds();

    void setReadGroupSetIds(String readGroupSetId);

    @Description("The ID of the Google Genomics Dataset that the pipeline will get its input reads"
        + " from.  Default (empty) means to use ReadGroupSetIds instead.  This or ReadGroupSetIds"
        + " must be set.  InputDatasetId overrides ReadGroupSetIds (if this field is set, "
        + "ReadGroupSetIds will be ignored).  All of the referenceSetIds for all ReadGroupSets in"
        + " this Dataset must be the same for the purposes of setting the referenceSetId"
        + " of the output AnnotationSet.")
    @Default.String("")
    String getInputDatasetId();

    void setInputDatasetId(String inputDatasetId);

    @Description("The ID of the Google Genomics Dataset that the output AnnotationSet will be "
        + "posted to.  If one is not specified, the InputDatasetId or the DatasetId field will be "
        + "used (Defaults to 1000 Genomes).")
    @Default.String("")
    String getOutputDatasetId();

    void setOutputDatasetId(String outputDatasetId);

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

    @Default.Boolean(true)
    Boolean getUseAlpn();

    void setUseAlpn(Boolean value);
  }

  public static void main(String[] args) throws GeneralSecurityException, IOException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(CoverageOptions.class);
    options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CoverageOptions.class);
    // Option validation is not yet automatic, we make an explicit call here.
    GenomicsDatasetOptions.Methods.validateOptions(options);
    auth = GenomicsOptions.Methods.getGenomicsAuth(options);

    // Must call getUseAlpn in order for it to be serialized.
    LOG.info("use_alpn: " + options.getUseAlpn().toString());

    p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);
    DataflowWorkarounds.registerCoder(p, PosRgsMq.class, GenericJsonCoder.of(PosRgsMq.class));
    DataflowWorkarounds.registerCoder(p, Read.class, SerializableCoder.of(Read.class));
    DataflowWorkarounds.registerCoder(p, StreamReadsRequest.class,
        Proto2Coder.of(StreamReadsRequest.class));

    if (options.getInputDatasetId().isEmpty() && options.getReadGroupSetIds().isEmpty()) {
      throw new IllegalArgumentException("InputDatasetId or ReadGroupSetIds must be specified");
    }

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

    AnnotationSet annotationSet = createAnnotationSet(checkReferenceSetIds(rgsIds));

    PCollection<Read> reads = getReadsFromAPI(rgsIds);
    PCollection<KV<PosRgsMq, Double>> coverageMeans = reads.apply(new CalculateCoverageMean());
    PCollection<KV<Position, KV<PosRgsMq.MappingQuality, List<Double>>>> quantiles
        = coverageMeans.apply(new CalculateQuantiles(options.getNumQuantiles()));
    PCollection<KV<Position, Iterable<KV<PosRgsMq.MappingQuality, List<Double>>>>> answer =
        quantiles.apply(GroupByKey.<Position, KV<PosRgsMq.MappingQuality, List<Double>>>create());
    answer.apply(ParDo.of(new CreateAnnotations(annotationSet.getId(), auth, true)));

    p.run();
  }

  /**
   * This step exists to emit the individual reads in a parallel step to the StreamReads step in
   * order to increase throughput.
   */
  static class ConvergeList extends DoFn<List<Read>, Read> {

    @Override
    public void processElement(ProcessContext c) {
      for (Read r : c.element()) {
        c.output(r);
      }
    }
  }

  /**
   * Composite PTransform that takes in Reads read in from various ReadGroupSetIds using the
   * Genomics API and calculates the coverage mean at every distinct PosRgsMq object for the data
   * input (Position, ReadgroupSetId, and Mapping Quality).
   */
  public static class CalculateCoverageMean extends PTransform<PCollection<Read>,
      PCollection<KV<PosRgsMq, Double>>> {

    @Override
    public PCollection<KV<PosRgsMq, Double>> apply(PCollection<Read> input) {
      return input.apply(ParDo.of(new CoverageCounts()))
          .apply(Combine.<PosRgsMq, Long>perKey(new SumCounts()))
          .apply(ParDo.of(new CoverageMeans()));
    }
  }

  static class CoverageCounts extends DoFn<Read, KV<PosRgsMq, Long>> {

    private static final int LOW_MQ = 10;
    private static final int HIGH_MQ = 30;

    @Override
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
        // Get options for getting references/bucket width
        CoverageOptions pOptions = c.getPipelineOptions().as(CoverageOptions.class);
        // Calculate readEnd by shifting readStart by readLength
        long readStart = c.element().getAlignment().getPosition().getPosition();
        long readEnd = readStart + readLength;
        // Get starting and ending references
        long refStart = 0;
        long refEnd = Long.MAX_VALUE;
        if (!pOptions.isAllReferences()) {
          // If we aren't using all references, parse the references to get proper start and end
          Iterable<Contig> contigs = Contig.parseContigsFromCommandLine(pOptions.getReferences());
          for (Contig ct : contigs) {
            if (ct.referenceName
                .equals(c.element().getAlignment().getPosition().getReferenceName())) {
              refStart = ct.start;
              refEnd = ct.end;
            }
          }
        }
        // Calculate the index of the first bucket this read falls into
        long bucket = ((readStart - refStart)
            / pOptions.getBucketWidth()) * pOptions.getBucketWidth() + refStart;
        long readCurr = readStart;
        if (readStart < refStart) {
          // If the read starts before the first bucket, start our calculations at the start of
          // the reference, since we know the read has to overlap there or else it wouldn't
          // be in our data set.
          bucket = refStart;
          readCurr = refStart;
        }
        while (readCurr < readEnd && readCurr < refEnd) {
          // Loop over read to generate output for each bucket
          long baseCount = 0;
          long dist = Math.min(bucket + pOptions.getBucketWidth(), readEnd) - readCurr;
          readCurr += dist;
          baseCount += dist;
          Position p = new Position()
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
          c.output(KV.of(new PosRgsMq(p, c.element().getReadGroupSetId(), mqEnum), baseCount));
          c.output(KV.of(new PosRgsMq(
              p, c.element().getReadGroupSetId(), PosRgsMq.MappingQuality.A), baseCount));
          bucket += pOptions.getBucketWidth();
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

    @Override
    public void processElement(ProcessContext c) {
      KV ans = KV.of(c.element().getKey(),
          (double) c.element().getValue()
          / (double) c.getPipelineOptions().as(CoverageOptions.class).getBucketWidth());
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
    public PCollection<KV<Position, KV<PosRgsMq.MappingQuality, List<Double>>>> apply(
        PCollection<KV<PosRgsMq, Double>> input) {
      return input.apply(ParDo.of(new RemoveRgsId()))
          .apply(ApproximateQuantiles
              .<KV<Position, PosRgsMq.MappingQuality>, Double>perKey(numQuantiles))
          .apply(ParDo.of(new MoveMapping()));
    }
  }

  static class RemoveRgsId extends DoFn<KV<PosRgsMq, Double>,
      KV<KV<Position, PosRgsMq.MappingQuality>, Double>> {

    @Override
    public void processElement(ProcessContext c) {
      KV ans = KV.of(KV.of(c.element().getKey().getPos(), c.element().getKey().getMq()),
          c.element().getValue());
      c.output(ans);
    }
  }

  static class MoveMapping extends DoFn<KV<KV<Position, PosRgsMq.MappingQuality>, List<Double>>,
      KV<Position, KV<PosRgsMq.MappingQuality, List<Double>>>> {

    @Override
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
    private final GenomicsFactory.OfflineAuth auth;
    private final List<Annotation> currAnnotations;
    private final boolean write;

    public CreateAnnotations(String asId, GenomicsFactory.OfflineAuth auth, boolean write) {
      this.asId = asId;
      this.auth = auth;
      this.currAnnotations = Lists.newArrayList();
      this.write = write;
    }

    @Override
    public void processElement(ProcessContext c) throws GeneralSecurityException, IOException {
      CoverageOptions pOptions = c.getPipelineOptions().as(CoverageOptions.class);
      Position bucket = c.element().getKey();
      Annotation a = new Annotation()
          .setAnnotationSetId(asId)
          .setPosition(new RangePosition()
              .setStart(bucket.getPosition())
              .setEnd(bucket.getPosition() + pOptions.getBucketWidth())
              .setReferenceName(bucket.getReferenceName()))
          .setType("GENERIC")
          .setInfo(new HashMap<String, List<String>>());
      for (KV<PosRgsMq.MappingQuality, List<Double>> p : c.element().getValue()) {
        List<String> output = Lists.newArrayList();
        for (int i = 0; i < p.getValue().size(); i++) {
          double value = Math.round(p.getValue().get(i) * 1000000.0) / 1000000.0;
          output.add(Double.toString(value));
        }
        a.getInfo().put(p.getKey().toString(), output);
      }
      if (write) {
        currAnnotations.add(a);
        if (currAnnotations.size() == 4096) {
          // Batch create annotations once we hit the max amount for a batch
          batchCreateAnnotations();
        }
      }
      c.output(a);
    }

    @Override
    public void finishBundle(Context c) throws IOException, GeneralSecurityException {
      // Finish up any leftover annotations at the end.
      if (write && !currAnnotations.isEmpty()) {
        batchCreateAnnotations();
      }
    }

    private void batchCreateAnnotations() throws IOException, GeneralSecurityException {
      Genomics.Annotations.BatchCreate aRequest = auth.getGenomics(auth.getDefaultFactory())
          .annotations().batchCreate(
              new BatchCreateAnnotationsRequest().setAnnotations(currAnnotations));
      RetryPolicy retryP = RetryPolicy.nAttempts(4);
      retryP.execute(aRequest);
      currAnnotations.clear();
    }
  }

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

  private static AnnotationSet createAnnotationSet(String referenceSetId)
      throws GeneralSecurityException, IOException {
    if (referenceSetId == null) {
      throw new IOException("ReferenceSetId was null for this readgroupset");
    }
    // Create a new annotation set using the given datasetId, or the one that this data was from if
    // one was not provided
    AnnotationSet as = new AnnotationSet();
    if (options.getOutputDatasetId().isEmpty() && options.getInputDatasetId().isEmpty()) {
      as.setDatasetId(options.getDatasetId());
    } else if (options.getOutputDatasetId().isEmpty()) {
      as.setDatasetId(options.getInputDatasetId());
    } else {
      as.setDatasetId(options.getOutputDatasetId());
    }
    if (!options.isAllReferences()) {
      as.setName("Read Depth for " + options.getReferences());
    } else {
      as.setName("Read Depth");
    }
    as.setReferenceSetId(referenceSetId);
    as.setType("GENERIC");
    Genomics.AnnotationSets.Create asRequest = auth.getGenomics(auth.getDefaultFactory())
        .annotationSets().create(as);
    AnnotationSet asWithId = asRequest.execute();
    return asWithId;
  }

  private static PCollection<Read> getReadsFromAPI(List<String> rgsIds)
      throws IOException, GeneralSecurityException {
    List<StreamReadsRequest> requests = options.isAllReferences() ?
        ShardUtils.getReadRequests(rgsIds) :
          ShardUtils.getReadRequests(rgsIds, options.getReferences(), options.getBasesPerShard());
    PCollection<StreamReadsRequest> readRequests = p.begin().apply(Create.of(requests));
    return readRequests.apply(new ReadStreamer(auth));
  }
}
