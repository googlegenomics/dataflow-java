/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dataflow.functions;

import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.ComposeRequest.SourceObjects;
import com.google.api.services.storage.model.StorageObject;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects.Compose;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.genomics.dataflow.readers.bam.BAMIO;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import htsjdk.samtools.BAMIndexer;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.BlockCompressedStreamConstants;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/*
 * Takes a set of BAM files that have been written to disk, concatenates them into one
 * file (removing unneeded EOF blocks), and writes an index for the combined file.
 */
public class CombineShardsFn extends DoFn<String, String> {

  public static interface Options extends GenomicsDatasetOptions, GCSOptions {}

  private static final int MAX_FILES_FOR_COMPOSE = 32;
  private static final String BAM_INDEX_FILE_MIME_TYPE = "application/octet-stream";
  private static final Logger LOG = Logger.getLogger(CombineShardsFn.class.getName());

  final PCollectionView<Iterable<String>> shards;
  
  public CombineShardsFn(PCollectionView<Iterable<String>> shards) {
    this.shards = shards;
  }
  
  @Override
  public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
    final String result = 
        combineShards(
            c.getPipelineOptions().as(Options.class), 
            c.element(),
            c.sideInput(shards));
    c.output(result);
  }

  static String combineShards(Options options, String dest,
      Iterable<String> shards) throws IOException {
    LOG.info("Combining shards into " + dest);
    final Storage.Objects storage = Transport.newStorageClient(
        options
          .as(GCSOptions.class))
          .build()
          .objects();

    ArrayList<String> sortedShardsNames = Lists.newArrayList(shards);
    Collections.sort(sortedShardsNames);

    // Write an EOF block (empty gzip block), and put it at the end.
    String eofFileName = options.getOutput() + "-EOF";
    final OutputStream os = Channels.newOutputStream(
        (new GcsUtil.GcsUtilFactory()).create(options).create(
            GcsPath.fromUri(eofFileName),
        BAM_INDEX_FILE_MIME_TYPE));
    os.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
    os.close();
    sortedShardsNames.add(eofFileName);
    
    int stageNumber = 0;
    /*
     * GCS Compose method takes only up to 32 files, so if we have more
     * shards than that we need to do a hierarchical combine: 
     * first combine all original shards in groups no more than 32
     * and then collect the results of these combines and so on until
     * we have a group of no more than 32 that we can finally combine into
     * a single file.
     */
    while (sortedShardsNames.size() > MAX_FILES_FOR_COMPOSE) {
      LOG.info("Stage " + stageNumber + ": Have " + sortedShardsNames.size() + 
          " shards: must combine in groups of " + MAX_FILES_FOR_COMPOSE);
      final ArrayList<String> combinedShards = Lists.newArrayList();
      for (int idx = 0; idx < sortedShardsNames.size(); idx += MAX_FILES_FOR_COMPOSE) {
        final int endIdx = Math.min(idx + MAX_FILES_FOR_COMPOSE, 
            sortedShardsNames.size());
        final List<String> combinableShards = sortedShardsNames.subList(
            idx, endIdx);
        final String intermediateCombineResultName = dest + "-" +
            String.format("%02d",stageNumber) + "-" + 
            String.format("%02d",idx) + "-" +
            String.format("%02d",endIdx);
        final String combineResult = composeAndCleanUpShards(storage,
            combinableShards, intermediateCombineResultName);
        combinedShards.add(combineResult);
        LOG.info("Stage " + stageNumber + ": adding combine result for " +
            idx + "-" + endIdx + ": " + combineResult);
      }
      LOG.info("Stage " + stageNumber + ": moving to next stage with " + 
          combinedShards.size() + "shards");
      sortedShardsNames = combinedShards;
      stageNumber++;
    }
    
    LOG.info("Combining a final group of " + sortedShardsNames.size() + " shards");
    final String combineResult = composeAndCleanUpShards(storage,
        sortedShardsNames, dest);
    generateIndex(options, storage, combineResult);
    return combineResult;
  }
  
 static void generateIndex(Options options, 
     Storage.Objects storage, String bamFilePath) throws IOException {
    final String baiFilePath = bamFilePath + ".bai";
    Stopwatch timer = Stopwatch.createStarted();
    LOG.info("Generating BAM index: " + baiFilePath);
    LOG.info("Reading BAM file: " + bamFilePath);
    final SamReader reader = BAMIO.openBAM(storage, bamFilePath, ValidationStringency.LENIENT, true);      
    
    final OutputStream outputStream =
        Channels.newOutputStream(
            new GcsUtil.GcsUtilFactory().create(options)
              .create(GcsPath.fromUri(baiFilePath),
                  BAM_INDEX_FILE_MIME_TYPE));
    BAMIndexer indexer = new BAMIndexer(outputStream, reader.getFileHeader());

    long processedReads = 0;

    // create and write the content
    for (SAMRecord rec : reader) {
        if (++processedReads % 1000000 == 0) {
          dumpStats(processedReads, timer);
        }
        indexer.processAlignment(rec);
    }
    indexer.finish();
    dumpStats(processedReads, timer);
  }
 
   static void dumpStats(long processedReads, Stopwatch timer) {
     LOG.info("Processed " + processedReads + " records in " + timer + 
         ". Speed: " + processedReads/timer.elapsed(TimeUnit.SECONDS) + " reads/sec");

   }
   
   static String composeAndCleanUpShards(Storage.Objects storage, 
       List<String> shardNames, String dest) throws IOException {
     LOG.info("Combining shards into " + dest);
   
     final GcsPath destPath = GcsPath.fromUri(dest);

     StorageObject destination = new StorageObject()
       .setContentType(BAM_INDEX_FILE_MIME_TYPE);
     
     ArrayList<SourceObjects> sourceObjects = new ArrayList<SourceObjects>();
     int addedShardCount = 0;
     for (String shard : shardNames) {
         final GcsPath shardPath = GcsPath.fromUri(shard);
         LOG.info("Adding shard " + shardPath + " for result " + dest);
         sourceObjects.add( new SourceObjects().setName(shardPath.getObject()));
         addedShardCount++;
     }
     LOG.info("Added " + addedShardCount + " shards for composition");

     final ComposeRequest composeRequest = new ComposeRequest()
       .setDestination(destination)
       .setSourceObjects(sourceObjects);
     final Compose compose = storage.compose(
         destPath.getBucket(), destPath.getObject(), composeRequest);
     final StorageObject result = compose.execute();
     final String combineResult =  GcsPath.fromObject(result).toString();
     LOG.info("Combine result is " + combineResult);
     for (SourceObjects sourceObject : sourceObjects) {
       final String shardToDelete = sourceObject.getName();
       LOG.info("Cleaning up shard  " + shardToDelete + " for result " + dest);
       storage.delete(destPath.getBucket(), shardToDelete).execute();
     }
     
     return combineResult;
   }
}