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
package com.google.cloud.genomics.dataflow.functions;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import com.google.cloud.genomics.utils.Contig;
import com.google.genomics.v1.StreamReadsRequest;
import com.google.genomics.v1.StreamVariantsRequest;

import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Convert genomic sites to shards.
 *
 * This input could, for example come from a BED file
 * https://genome.ucsc.edu/FAQ/FAQformat.html#format1
 *
 * It could also come from a quick query on a BigQuery table, exporting the three columns to Cloud
 * Storage as a CSV file. (or this could be expanded to read from BigQuery table rows)
 *
 * Each line consists of three fields (any additional fields will be ignored):
 * <ol>
 * <li>reference name (e.g., chrX)
 * <li>the starting position in 0-based coordinates
 * <li>the ending position in 0-based coordinates
 * </ol>
 *
 * The fields may be comma, tab, or whitespace delimited.
 */
public class SitesToShards {

  public static interface Options extends PipelineOptions {
    @Description("Path to the 'sites' file, where each line contains three comma, tab or whitespace delimited "
        + "fields (1) reference name (e.g., chrX or X) (2) the starting position in 0-based coordinates "
        + "(3) the ending position in 0-based coordinates.  Any additional fields will be ignored. "
        + "This could be a BED file or the output of a BigQuery query materialized to Cloud Storage as a CSV.")
    String getSitesFilepath();
    void setSitesFilepath(String sitesFilePath);
  }

  private static final Logger LOG = Logger.getLogger(SitesToShards.class.getName());

  private static final Pattern SITE_PATTERN = Pattern.compile("^\\s*([\\w\\.]+)\\W+(\\d+)\\W+(\\d+).*$");

  /**
   * Given a string encoding a site, parse it into a Contig object.
   */
  public static class SitesToContigsFn extends DoFn<String, Contig> {

    @ProcessElement
    public void processElement(DoFn<String, Contig>.ProcessContext context) throws Exception {
      String line = context.element();
      Matcher m = SITE_PATTERN.matcher(line);
      if (m.matches()) {
        context.output(new Contig(m.group(1),
            Integer.parseInt(m.group(2)),
            Integer.parseInt(m.group(3))));
      }
      // e.g., If we pass an actual BED file, it may have some header lines.
      LOG.warning("Skipping line from sites file: " + line);
    }
  }

  /**
   * Given a contig object and request prototype, construct a request spanning the region
   * defined by the contig.
   */
  public static class ContigsToStreamVariantsRequestsFn extends
      SimpleFunction<Contig, StreamVariantsRequest> {

    private final StreamVariantsRequest prototype;

    public ContigsToStreamVariantsRequestsFn(StreamVariantsRequest prototype) {
      super();
      this.prototype = prototype;
    }

    @Override
    public StreamVariantsRequest apply(Contig contig) {
      if (null == contig) {
        return null;
      }
      return contig.getStreamVariantsRequest(prototype);
    }

  }

  /**
   * Use this transform when you have file(s) of sites that should be converted into
   * streaming requests that each span the region for a site.
   */
  public static class SitesToStreamVariantsShardsTransform extends
      PTransform<PCollection<String>, PCollection<StreamVariantsRequest>> {

    private final StreamVariantsRequest prototype;

    public SitesToStreamVariantsShardsTransform(StreamVariantsRequest prototype) {
      super();
      this.prototype = prototype;
    }

    @Override
    public PCollection<StreamVariantsRequest> expand(PCollection<String> lines) {
      return lines.apply(ParDo.of(new SitesToContigsFn()))
          .apply("Contigs to StreamVariantsRequests",
          MapElements.via(new ContigsToStreamVariantsRequestsFn(prototype)));
    }
  }

  /**
   * Given a contig object and request prototype, construct a request spanning the region
   * defined by the contig.
   */
  public static class ContigsToStreamReadsRequestsFn extends
  SimpleFunction<Contig, StreamReadsRequest> {

    private final StreamReadsRequest prototype;

    public ContigsToStreamReadsRequestsFn(StreamReadsRequest prototype) {
      super();
      this.prototype = prototype;
    }

    @Override
    public StreamReadsRequest apply(Contig contig) {
      if (null == contig) {
        return null;
      }
      return contig.getStreamReadsRequest(prototype);
    }

  }

  /**
   * Use this transform when you have file(s) of sites that should be converted into
   * streaming requests that each span the region for a site.
   */
  public static class SitesToStreamReadsShardsTransform extends
  PTransform<PCollection<String>, PCollection<StreamReadsRequest>> {

    private final StreamReadsRequest prototype;

    public SitesToStreamReadsShardsTransform(StreamReadsRequest prototype) {
      super();
      this.prototype = prototype;
    }

    @Override
    public PCollection<StreamReadsRequest> expand(PCollection<String> lines) {
      return lines.apply(ParDo.of(new SitesToContigsFn()))
          .apply("Contigs to StreamReadsRequests",
              MapElements.via(new ContigsToStreamReadsRequestsFn(prototype)));
    }
  }
}
