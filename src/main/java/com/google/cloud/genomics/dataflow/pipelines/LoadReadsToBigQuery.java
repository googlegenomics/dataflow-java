/*
 * Copyright (C) 2018 Google Inc.
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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.genomics.dataflow.readers.bam.BAMShard;
import com.google.cloud.genomics.dataflow.readers.bam.ReadBAMTransform;
import com.google.cloud.genomics.dataflow.readers.bam.Reader;
import com.google.cloud.genomics.dataflow.readers.bam.ReaderOptions;
import com.google.cloud.genomics.dataflow.readers.bam.ShardingPolicy;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.genomics.v1.CigarUnit;
import com.google.genomics.v1.Read;
import htsjdk.samtools.ValidationStringency;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.PCollection;

/**
 * Pipeline for loading BAM file (hosted on GCS) into BigQuery.
 *
 */
public class LoadReadsToBigQuery {
  /**
   * Pipeline options.
   */
  public static interface Options extends GCSOptions, ShardOptions {

    @Description("The Google Cloud Storage path to the BAM file to get reads data from.")
    @Default.String("")
    String getBamFilePath();
    void setBamFilePath(String filePath);

    @Description("For sharded BAM file input, the maximum size in megabytes of a shard processed "
        + "by a single worker. A non-positive value implies no sharding.")
    @Default.Integer(100)
    int getMaxShardSizeMB();
    void setMaxShardSizeMB(int maxShardSizeMB);

    @Description("Whether to include unmapped mate pairs of mapped reads to match expectations "
        + "of Picard tools.")
    @Default.Boolean(false)
    boolean isIncludeUnmapped();
    void setIncludeUnmapped(boolean newValue);

    @Description("BigQuery table (in [PROJECT]:[DATASET].[TABLE] format) to store the result.")
    @Default.String("")
    String getBigQueryTablePath();
    void setBigQueryTablePath(String tablePath);

    @Description("Whether to wait until the pipeline completes. This is useful for test purposes.")
    @Default.Boolean(false)
    boolean getWait();
    void setWait(boolean wait);

    @Validation.Required
    @Description("Google Cloud Storage path to write temp files, if applicable.")
    String getTempPath();
    void setTempPath(String tempPath);

    /**
     * Class to validate options.
     */
    public static class Methods {
      public static void validateOptions(Options options) {
        // Validate tempPath.
        try {
          // Check that we can parse the path.
          GcsPath valid = GcsPath.fromUri(options.getTempPath());
          // GcsPath allows for empty bucket, but that doesn't make for a good temp path.
          Preconditions.checkArgument(!Strings.isNullOrEmpty(valid.getBucket()),
              "Bucket must be specified");
        } catch (IllegalArgumentException x) {
          Preconditions.checkState(false, "the specified temp path is invalid: " + x.getMessage());
        }
      }
    }
  }

  // BigQuery schema field type.
  private static final String BOOL = "BOOL";
  private static final String INT64 = "INT64";
  private static final String STRING = "STRING";
  private static final String RECORD = "RECORD";

  // BigQuery schema field mode.
  private static final String REPEATED = "REPEATED";

  // BigQuery schema field names.
  private static final String ALIGNED_SEQUENCE = "aligned_sequence";
  private static final String ALIGNED_QUALITY = "aligned_quality";
  private static final String ALIGNMENT = "alignment";
  private static final String CIGAR = "cigar";
  private static final String DUPLICATE_FRAGMENT = "duplicate_fragment";
  private static final String FAILED_VENDOR_QUALITY_CHECKS = "failed_vendor_quality_checks";
  private static final String FRAGMENT_LENGTH = "fragment_length";
  private static final String FRAGMENT_NAME = "fragment_name";
  private static final String ID = "id";
  private static final String MAPPING_QUALITY = "mapping_quality";
  private static final String NEXT_MATE_POSITION = "next_mate_position";
  private static final String NUMBER_READS = "number_reads";
  private static final String OPERATION = "operation";
  private static final String OPERATION_LENGTH = "operation_length";
  private static final String OPERATION_SEQUENCE = "operation_sequence";
  private static final String POSITION = "position";
  private static final String PROPER_PLACEMENT = "proper_placement";
  private static final String READ_GROUP_ID = "read_group_id";
  private static final String READ_GROUP_SET_ID = "read_group_set_id";
  private static final String READ_NUMBER = "read_number";
  private static final String REFERENCE_NAME = "reference_name";
  private static final String REFERENCE_SEQUENCE = "reference_sequence";
  private static final String REVERSE_STRAND = "reverse_strand";
  private static final String SECONDARY_ALIGNMENT = "secondary_alignment";
  private static final String SUPPLEMENTARY_ALIGNMENT = "supplementary_alignment";

  private static final Logger LOG = Logger.getLogger(LoadReadsToBigQuery.class.getName());
  private static Pipeline p;
  private static Options pipelineOptions;
  private static OfflineAuth auth;


  /**
   * Converts Reads into BigQuery rows.
   */
  static class ReadToRowConverter extends DoFn<Read, TableRow> {
   static TableFieldSchema getPositionFieldSchema(String fieldName) {
      TableFieldSchema fieldSchema =
          new TableFieldSchema().setName(fieldName).setType(RECORD);
      fieldSchema.setFields(new ArrayList<TableFieldSchema>() {
        private static final long serialVersionUID = 0;
        {
          add(new TableFieldSchema().setName(REFERENCE_NAME).setType(STRING));
          add(new TableFieldSchema().setName(POSITION).setType(INT64));
          add(new TableFieldSchema().setName(REVERSE_STRAND).setType(BOOL));
        }
      });
      return fieldSchema;
    }

    static TableFieldSchema getAlignmentFieldSchema(String fieldName) {
      TableFieldSchema fieldSchema =
          new TableFieldSchema().setName(fieldName).setType(RECORD);
      fieldSchema.setFields(new ArrayList<TableFieldSchema>() {
        private static final long serialVersionUID = 0;
        {
          // Position record.
          add(getPositionFieldSchema(POSITION));
          add(new TableFieldSchema().setName(MAPPING_QUALITY).setType(INT64));
          // CIGAR record.
          TableFieldSchema cigarSchema =
              new TableFieldSchema().setName(CIGAR).setType(RECORD).setMode(REPEATED);
          cigarSchema.setFields(new ArrayList<TableFieldSchema>() {
            private static final long serialVersionUID = 0;
            {
              // Operation is of type enum, but BQ doesn't accept enum. Hence, converting to INT64.
              add(new TableFieldSchema().setName(OPERATION).setType(INT64));
              add(new TableFieldSchema().setName(OPERATION_LENGTH).setType(INT64));
              add(new TableFieldSchema().setName(REFERENCE_SEQUENCE).setType(STRING));
            }
          });
          add(cigarSchema);
        }
      });
      return fieldSchema;
    }

    static TableSchema getSchema() {
      return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
        private static final long serialVersionUID = 0;
        // Compose the list of TableFieldSchema from tableSchema.
        {
          add(new TableFieldSchema().setName(ID).setType(STRING));
          add(new TableFieldSchema().setName(READ_GROUP_ID).setType(STRING));
          add(new TableFieldSchema().setName(READ_GROUP_SET_ID).setType(STRING));
          add(new TableFieldSchema().setName(FRAGMENT_NAME).setType(STRING));
          add(new TableFieldSchema().setName(PROPER_PLACEMENT).setType(BOOL));
          add(new TableFieldSchema().setName(DUPLICATE_FRAGMENT).setType(BOOL));
          add(new TableFieldSchema().setName(FRAGMENT_LENGTH).setType(INT64));
          add(new TableFieldSchema().setName(READ_NUMBER).setType(INT64));
          add(new TableFieldSchema().setName(NUMBER_READS).setType(INT64));
          add(new TableFieldSchema().setName(FAILED_VENDOR_QUALITY_CHECKS).setType(BOOL));
          add(getAlignmentFieldSchema(ALIGNMENT));
          add(new TableFieldSchema().setName(SECONDARY_ALIGNMENT).setType(BOOL));
          add(new TableFieldSchema().setName(SUPPLEMENTARY_ALIGNMENT).setType(BOOL));
          add(new TableFieldSchema().setName(ALIGNED_SEQUENCE).setType(STRING));
          add(new TableFieldSchema().setName(ALIGNED_QUALITY).setType(INT64).setMode(REPEATED));
          add(getPositionFieldSchema(NEXT_MATE_POSITION));
          // TODO(#222): store info field into bigquery.
        }
      });
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      TableRow row = new TableRow()
          .set(ID, c.element().getId())
          .set(READ_GROUP_ID, c.element().getReadGroupId())
          .set(READ_GROUP_SET_ID, c.element().getReadGroupSetId())
          .set(FRAGMENT_NAME, c.element().getFragmentName())
          .set(PROPER_PLACEMENT, c.element().getProperPlacement())
          .set(DUPLICATE_FRAGMENT, c.element().getDuplicateFragment())
          .set(FRAGMENT_LENGTH, c.element().getFragmentLength())
          .set(READ_NUMBER, c.element().getReadNumber())
          .set(NUMBER_READS, c.element().getNumberReads())
          .set(FAILED_VENDOR_QUALITY_CHECKS, c.element().getFailedVendorQualityChecks())
          .set(SECONDARY_ALIGNMENT, c.element().getSecondaryAlignment())
          .set(SUPPLEMENTARY_ALIGNMENT, c.element().getSupplementaryAlignment())
          .set(ALIGNED_SEQUENCE, c.element().getAlignedSequence())
          .set(ALIGNED_QUALITY, c.element().getAlignedQualityList())
          .set(NEXT_MATE_POSITION,
              new TableRow().set(REFERENCE_NAME,
                                 c.element().getNextMatePosition().getReferenceName())
                            .set(POSITION, c.element().getNextMatePosition().getPosition())
                            .set(REVERSE_STRAND,
                                 c.element().getNextMatePosition().getReverseStrand())
               );
      // Alignment
      TableRow alignmentPosition =
          new TableRow().set(REFERENCE_NAME,
                             c.element().getAlignment().getPosition().getReferenceName())
                        .set(POSITION, c.element().getAlignment().getPosition().getPosition())
                        .set(REVERSE_STRAND,
                             c.element().getAlignment().getPosition().getReverseStrand());
      List<TableRow> alignmentCigars = new ArrayList<>();
      for (CigarUnit cigar : c.element().getAlignment().getCigarList()) {
        TableRow alignmentCigar =
            new TableRow().set(OPERATION, cigar.getOperation().getNumber())
                          .set(OPERATION_LENGTH, cigar.getOperationLength())
                          .set(REFERENCE_SEQUENCE, cigar.getReferenceSequence());
        alignmentCigars.add(alignmentCigar);
      }
      TableRow alignmentRecord = new TableRow().set(POSITION, alignmentPosition)
          .set(MAPPING_QUALITY, c.element().getAlignment().getMappingQuality())
          .set(CIGAR, alignmentCigars);
      row.set(ALIGNMENT, alignmentRecord);

      c.output(row);
    }
  }

  public static void main(String[] args) throws GeneralSecurityException, IOException,
         URISyntaxException {
    // Register the options so that they show up via --help.
    PipelineOptionsFactory.register(Options.class);
    pipelineOptions = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    Options.Methods.validateOptions(pipelineOptions);
    // Needed for BigQuery.
    pipelineOptions.setTempLocation(pipelineOptions.getTempPath());

    auth = GenomicsOptions.Methods.getGenomicsAuth(pipelineOptions);
    p = Pipeline.create(pipelineOptions);

    // Ensure data is accessible.
    String BamFilePath = pipelineOptions.getBamFilePath();
    if (!Strings.isNullOrEmpty(BamFilePath)) {
      try {
        checkGcsUrlExists(BamFilePath);
      } catch (Exception x) {
        System.err.println("Error: BAM file " + BamFilePath + " not found. A BAM file is "
            + "required.");
        System.err.println("Exception: " + x.getMessage());
        return;
      }
      if (pipelineOptions.getMaxShardSizeMB() > 0) {
        // The BAM code expects an index at BamFilePath+".bai"
        // and sharded reading will fail if the index isn't there.
        String BamIndexPath = BamFilePath + ".bai";
        try {
          checkGcsUrlExists(BamIndexPath);
        } catch (Exception x) {
          System.err.println("Error: Index file " + BamIndexPath + " not found. An index is "
              + "required for sharded export.");
          System.err.println("Exception: " + x.getMessage());
          return;
        }
      }
    }

    PCollection<Read> reads = getReadsFromBamFile();
    // TODO(#221): validate the given BigQuery table, and fail fast if it is not valid and empty.
    reads.apply(ParDo.of(new ReadToRowConverter()))
        .apply(BigQueryIO.writeTableRows()
            .to(pipelineOptions.getBigQueryTablePath())
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withSchema(ReadToRowConverter.getSchema()));

    PipelineResult result = p.run();
    if (pipelineOptions.getWait()) {
      result.waitUntilFinish();
    }
  }

  private static void checkGcsUrlExists(String url) throws IOException {
    // Ensure data is accessible.
    // If we can read the size, then surely we can read the file.
    GcsPath fn = GcsPath.fromUri(url);
    Storage.Objects storageClient = GCSOptions.Methods.createStorageClient(pipelineOptions, auth);
    Storage.Objects.Get getter = storageClient.get(fn.getBucket(), fn.getObject());
    StorageObject object = getter.execute();
    BigInteger size = object.getSize();
  }

  private static PCollection<Read> getReadsFromBamFile() throws IOException, URISyntaxException {
    LOG.info("getReadsFromBamFile");

    final Iterable<Contig> contigs =
        Contig.parseContigsFromCommandLine(pipelineOptions.getReferences());
    final ReaderOptions readerOptions = new ReaderOptions(
        ValidationStringency.LENIENT,
        pipelineOptions.isIncludeUnmapped());
    if (pipelineOptions.getMaxShardSizeMB() > 0) {
      LOG.info("Sharded reading of " + pipelineOptions.getBamFilePath());

      ShardingPolicy policy = new ShardingPolicy() {
        final int MAX_BYTES_PER_SHARD_IN_BYTES = pipelineOptions.getMaxShardSizeMB() * 1024 * 1024;
        @Override
        public Boolean apply(BAMShard shard) {
          return shard.approximateSizeInBytes() > MAX_BYTES_PER_SHARD_IN_BYTES;
        }
      };

      return ReadBAMTransform.getReadsFromBAMFilesSharded(p,
          pipelineOptions,
          auth,
          Lists.newArrayList(contigs),
          readerOptions,
          pipelineOptions.getBamFilePath(),
          policy);
    } else {  // For testing and comparing sharded vs. not sharded only.
      LOG.info("Unsharded reading of " + pipelineOptions.getBamFilePath());
      return p.apply(
          Create.of(
              Reader.readSequentiallyForTesting(
                  GCSOptions.Methods.createStorageClient(pipelineOptions, auth),
                  pipelineOptions.getBamFilePath(),
                  contigs.iterator().next(),
                  readerOptions)));
    }
  }
}
