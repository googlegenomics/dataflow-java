/*
 * Copyright (C) 2016 Google Inc.
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

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.genomics.Genomics;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import com.google.cloud.genomics.dataflow.utils.GCSOutputOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Given a file of variant ids, delete them from a variant set in parallel.
 *
 * These variant ids may be from the result of a BigQuery query materialized to Cloud Storage
 * as CSV or as the result of a Dataflow pipeline such as IdentifyPrivateVariants.
 */
public class DeleteVariants {

  public static interface Options extends GCSOutputOptions {
    @Description("The Cloud Storage filepath to a comma-separated or tab-separated file of variant ids. "
        + "The variant id will be retrieved from the first column.  Any other columns will be ignored. "
        + "The file should not include any header lines.")
    @Required
    String getInput();
    void setInput(String filepath);

    public static class Methods {
      public static void validateOptions(Options options) {
        GCSOutputOptions.Methods.validateOptions(options);
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DeleteVariants.class);

  public static final class DeleteVariantFn extends DoFn<String, Integer> {
    private final OfflineAuth auth;
    private Genomics genomics;

    public DeleteVariantFn(OfflineAuth auth) {
      super();
      this.auth = auth;
    }

    @StartBundle
    public void startBundle(StartBundleContext context) throws IOException, GeneralSecurityException {
      genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);
    }

    @ProcessElement
    public void processElement(DoFn<String, Integer>.ProcessContext context) throws Exception {
      String variantId = context.element();
      // Call the deletion operation via exponential backoff so that "Rate Limit Exceeded"
      // quota issues do not cause the pipeline to fail.
      ExponentialBackOff backoff = new ExponentialBackOff.Builder().build();
      while (true) {
        try {
          genomics.variants().delete(variantId).execute();
          Metrics.counter(DeleteVariantFn.class, "Number of variants deleted").inc();
          context.output(1);
          return;
        } catch (Exception e) {
          if (e.getMessage().startsWith("429 Too Many Requests")) {
            LOG.warn("Backing-off per: ", e);
            long backOffMillis = backoff.nextBackOffMillis();
            if (backOffMillis == BackOff.STOP) {
              throw e;
            }
            Thread.sleep(backOffMillis);
          } else {
            throw e;
          }
        }
      }
    }
  }

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(Options.class);
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    Options.Methods.validateOptions(options);

    OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);

    GenomicsOptions.Methods.requestConfirmation("*** The pipeline will delete variants whose "
        + "ids are listed in: " + options.getInput() + ". ***");

    Pipeline p = Pipeline.create(options);

    p.apply("ReadLines", TextIO.read().from(options.getInput()))
        .apply("ParseVariantIds", ParDo.of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            String record = c.element();

            // The variant id will be retrieved from the first column.  Any other columns
            // will be ignored.
            Iterable<String> fields =
                Splitter.on(
                    CharMatcher.BREAKING_WHITESPACE.or(CharMatcher.is(',')))
                    .omitEmptyStrings()
                    .trimResults()
                    .split(record);
            java.util.Iterator<String> iter = fields.iterator();
            if (iter.hasNext()) {
              c.output(iter.next());
            }
          }
        }))
        .apply(ParDo.of(new DeleteVariantFn(auth)))
        .apply(Sum.integersGlobally())
        .apply("FormatResults", ParDo.of(new DoFn<Integer, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output("Deleted Variant Count: " + c.element());
          }
        }))
        .apply("Write Count", TextIO.write().to(options.getOutput()));

    p.run();
  }
}
