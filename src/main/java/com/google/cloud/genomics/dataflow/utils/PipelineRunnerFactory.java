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
package com.google.cloud.genomics.dataflow.utils;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;

public enum PipelineRunnerFactory {

  BlockingDataflowPipelineRunner {
    @Override public com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
        createPipelineRunner(PipelineOptions options) {
      return com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
          .fromOptions(options);
    }
  },

  DataflowPipelineRunner {
    @Override public com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner
        createPipelineRunner(PipelineOptions options) {
      return com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner
          .fromOptions(options);
    }
  },

  DirectPipelineRunner {
    @Override public com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner
        createPipelineRunner(PipelineOptions options) {
      return com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner
          .fromOptions(options);
    }
  };

  public abstract PipelineRunner<?> createPipelineRunner(PipelineOptions options);
}
