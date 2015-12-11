/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.genomics.dataflow.readers;

import java.util.logging.Logger;

import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.Paginator;
import com.google.cloud.genomics.utils.ShardBoundary;

/**
 * ReadReader is a DoFn that takes SearchReadsRequests, submits them
 * to the Genomics API, and gives you Reads in return.
 * It will take care of paging for you. You can optionally specify
 * which fields you'd like returned.
 */
public class ReadReader extends GenomicsApiReader<SearchReadsRequest, Read> {
  private static final Logger LOG = Logger.getLogger(ReadReader.class.getName());
  private final ShardBoundary.Requirement shardBoundary;

  /**
   * Create a ReadReader using a auth and fields parameter. All fields not specified under 
   * readFields will not be returned in the API response.
   * 
   * @param auth Auth class containing credentials.
   * @param readFields Fields to return in responses.
   */
  public ReadReader(OfflineAuth auth, ShardBoundary.Requirement shardBoundary, String readFields) {
    super(auth, readFields);
    this.shardBoundary = shardBoundary;
  }

  /**
   * Create a ReadReader with no fields parameter, all information will be returned.
   * @param auth Auth class containing credentials.
   */
  public ReadReader(OfflineAuth auth, ShardBoundary.Requirement shardBoundary) {
    this(auth, shardBoundary, null);
  }

  @Override
  protected void processApiCall(Genomics genomics, ProcessContext c, SearchReadsRequest request) {
    LOG.info("Starting Reads read loop");
    
    GenomicsOptions options = c.getPipelineOptions().as(GenomicsOptions.class);
    if (options.getPageSize() > 0) {
      request.setPageSize(options.getPageSize());
    }

    for (Read read : Paginator.Reads.create(genomics, shardBoundary).search(request, fields)) {
      c.output(read);
      itemCount.addValue(1L);
    }
  }
}
