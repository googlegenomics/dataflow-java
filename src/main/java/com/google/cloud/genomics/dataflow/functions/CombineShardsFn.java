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

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects.Compose;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.ComposeRequest.SourceObjects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum.SumIntegerFn;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GCSOutputOptions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/*
 * Takes a set of files that have been written to disk, concatenates them into one
 * file and also appends "EOF" content at the end.
 */
public class CombineShardsFn extends DoFn<String, String> {

  public static interface Options extends GCSOutputOptions, GCSOptions {}

  private static final int MAX_FILES_FOR_COMPOSE = 32;
  private static final int MAX_RETRY_COUNT = 3;
  
  private static final String FILE_MIME_TYPE = "application/octet-stream";
  private static final Logger LOG = Logger.getLogger(CombineShardsFn.class.getName());

  final PCollectionView<Iterable<String>> shards;
  final PCollectionView<byte[]> eofContent;
  Aggregator<Integer, Integer> filesToCombineAggregator;
  Aggregator<Integer, Integer> combinedFilesAggregator;
  Aggregator<Integer, Integer> createdFilesAggregator;
  Aggregator<Integer, Integer> deletedFilesAggregator;
  
  public CombineShardsFn(PCollectionView<Iterable<String>> shards, PCollectionView<byte[]> eofContent) {
    this.shards = shards;
    this.eofContent = eofContent;
    filesToCombineAggregator = createAggregator("Files to combine", new SumIntegerFn());
    combinedFilesAggregator = createAggregator("Files combined", new SumIntegerFn());
    createdFilesAggregator = createAggregator("Created files", new SumIntegerFn());
    deletedFilesAggregator = createAggregator("Deleted files", new SumIntegerFn());
  }
  
  @Override
  public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
    final String result = 
        combineShards(
            c.getPipelineOptions().as(Options.class), 
            c.element(),
            c.sideInput(shards),
            c.sideInput(eofContent));
    c.output(result);
  }

  String combineShards(Options options, String dest,
      Iterable<String> shards, byte[] eofContent) throws IOException {
    LOG.info("Combining shards into " + dest);
    final Storage.Objects storage = Transport.newStorageClient(
        options
          .as(GCSOptions.class))
          .build()
          .objects();

    ArrayList<String> sortedShardsNames = Lists.newArrayList(shards);
    Collections.sort(sortedShardsNames);

    // Write an EOF block (empty gzip block), and put it at the end.
    if (eofContent != null && eofContent.length > 0) {
      String eofFileName = options.getOutput() + "-EOF";
      final OutputStream os = Channels.newOutputStream(
          (new GcsUtil.GcsUtilFactory()).create(options).create(
              GcsPath.fromUri(eofFileName),
              FILE_MIME_TYPE));
      os.write(eofContent);
      os.close();
      sortedShardsNames.add(eofFileName);
      LOG.info("Written " + eofContent.length + " bytes into EOF file " + 
          eofFileName);
    } else {
      LOG.info("No EOF content");
    }
    
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
    return combineResult;
  }
    
 String composeAndCleanUpShards(
      Storage.Objects storage, List<String> shardNames, String dest) throws IOException {
    LOG.info("Combining shards into " + dest);

    final GcsPath destPath = GcsPath.fromUri(dest);

    StorageObject destination = new StorageObject().setContentType(FILE_MIME_TYPE);

    ArrayList<SourceObjects> sourceObjects = new ArrayList<SourceObjects>();
    int addedShardCount = 0;
    for (String shard : shardNames) {
      final GcsPath shardPath = GcsPath.fromUri(shard);
      LOG.info("Adding shard " + shardPath + " for result " + dest);
      sourceObjects.add(new SourceObjects().setName(shardPath.getObject()));
      addedShardCount++;
    }
    LOG.info("Added " + addedShardCount + " shards for composition");
    filesToCombineAggregator.addValue(addedShardCount);
    
    final ComposeRequest composeRequest =
        new ComposeRequest().setDestination(destination).setSourceObjects(sourceObjects);
    final Compose compose =
        storage.compose(destPath.getBucket(), destPath.getObject(), composeRequest);
    final StorageObject result = compose.execute();
    final String combineResult = GcsPath.fromObject(result).toString();
    LOG.info("Combine result is " + combineResult);
    combinedFilesAggregator.addValue(addedShardCount);
    createdFilesAggregator.addValue(1);
    for (SourceObjects sourceObject : sourceObjects) {
      final String shardToDelete = sourceObject.getName();
      LOG.info("Cleaning up shard  " + shardToDelete + " for result " + dest);
      int retryCount = MAX_RETRY_COUNT;
      boolean done = false;
      while (!done && retryCount > 0) {
        try {
          storage.delete(destPath.getBucket(), shardToDelete).execute();
          done = true;
        } catch (Exception ex) {
          LOG.info("Error deleting " + ex.getMessage() + retryCount + " retries left");
        }
        retryCount--;
      }
      deletedFilesAggregator.addValue(1);
    }

    return combineResult;
  }
}