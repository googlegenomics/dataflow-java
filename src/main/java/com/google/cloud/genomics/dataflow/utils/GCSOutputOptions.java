/*
 * Copyright (C) 2014 Google Inc.
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
package com.google.cloud.genomics.dataflow.utils;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.common.base.Preconditions;

/**
 * A common options class for all pipelines that write their analysis results to GCS files.
 */
public interface GCSOutputOptions extends GenomicsOptions {

  public static class Methods {
    public static void validateOptions(GCSOutputOptions options) {
      try {
        // Check that we can parse the path.
        GcsPath valid = GcsPath.fromUri(options.getOutput());
        // GcsPath allows for empty bucket or filename, but that doesn't make for a good output file.
        Preconditions.checkArgument(!Strings.isNullOrEmpty(valid.getBucket()), "Bucket must be specified");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(valid.getObject()), "Filename prefix must be specified");
      } catch (Exception x) {
        Preconditions.checkState(false, "output must be a valid Google Cloud Storage URL (starting with gs://)");
      }
    }
  }

  @Validation.Required
  @Description("Google Cloud Storage path prefix of the files to which to write pipeline output, if applicable.")
  String getOutput();
  void setOutput(String output);
}
