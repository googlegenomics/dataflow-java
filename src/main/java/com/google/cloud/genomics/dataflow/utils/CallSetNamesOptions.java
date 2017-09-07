/*
 * Copyright (C) 2016 Google Inc.
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

import com.google.api.services.genomics.model.CallSet;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
import com.google.cloud.genomics.utils.CallSetUtils;
import com.google.cloud.genomics.utils.GenomicsUtils;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.genomics.v1.StreamVariantsRequest;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * A common options class for variant sets and call sets.
 */
public interface CallSetNamesOptions extends GenomicsOptions {

  @Required
  @Description("The ID of the Google Genomics variant set this pipeline is accessing.")
  String getVariantSetId();
  void setVariantSetId(String variantSetId);

  @Description("A comma-separated list of callset names. Use this option or "
      + "--callSetNamesFilepath to specify the subset of callsets over which "
      + "this pipeline should compute.")
  String getCallSetNames();
  void setCallSetNames(String callSetNames);

  @Description("A local file path to a list of newline-separated callset names. "
      + "Use this option or --callSetNames to specify the subset of callsets "
      + "over which this pipeline should compute.")
  String getCallSetNamesFilepath();
  void setCallSetNamesFilepath(String callSetNamesFilepath);

  public static class Methods {

    /**
     * Construct a request prototype with several fields already filled in using option values.
     *
     * @param options
     * @return the request prototype
     * @throws IOException
     */
    public static StreamVariantsRequest getRequestPrototype(final CallSetNamesOptions options) throws IOException {
      StreamVariantsRequest.Builder request = StreamVariantsRequest.newBuilder()
          .setVariantSetId(options.getVariantSetId())
          .addAllCallSetIds(getCallSetIds(options));

      if (null != options.getProject()) {
        request.setProjectId(options.getProject());
      }

      return request.build();
    }

    /**
     * Parse and return the unique call set names specified in the options.
     *
     * @param options
     * @return a list of unique call set names
     * @throws IOException
     */
    public static List<String> getCallSetNames(final CallSetNamesOptions options) throws IOException {
      Preconditions.checkArgument(
          null == options.getCallSetNames() || null == options.getCallSetNamesFilepath(),
          "Only specify one of --callSetNamesList or --callSetNamesFilepath");
      if (!Strings.isNullOrEmpty(options.getCallSetNamesFilepath())) {
        String fileContents =
            Files.toString(new File(options.getCallSetNamesFilepath()), Charset.defaultCharset());
        return ImmutableSet
            .<String>builder()
            .addAll(
                Splitter.on(CharMatcher.breakingWhitespace()).omitEmptyStrings().trimResults()
                    .split(fileContents)).build().asList();
      }
      if (!Strings.isNullOrEmpty(options.getCallSetNames())) {
        return ImmutableSet
            .<String>builder()
            .addAll(
                Splitter.on(CharMatcher.is(',')).omitEmptyStrings().trimResults()
                    .split(options.getCallSetNames())).build().asList();
      }
      return ImmutableSet.<String>builder().build().asList();
    }

    /**
     * Return the call set ids corresponding to the call set names provided in the options.
     *
     * This has a side-effect of confirming that the call set names within the variant set are unique.
     *
     * @param options
     * @return a list of unique call set ids
     * @throws IOException
     */
    public static List<String> getCallSetIds(final CallSetNamesOptions options) throws IOException {
      List<String> callSetNames = getCallSetNames(options);
      if (callSetNames.isEmpty()) {
        return callSetNames;  // Return the empty list.
      }

      ImmutableSet.Builder<String> callSetIds = ImmutableSet.<String>builder();
      Iterable<CallSet> callSets = GenomicsUtils.getCallSets(options.getVariantSetId(),
          GenomicsOptions.Methods.getGenomicsAuth(options));
      BiMap<String,String> nameToId = null;
      try {
        nameToId = CallSetUtils.getCallSetNameMapping(callSets);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("VariantSet " + options.getVariantSetId()
            + " contains duplicate callset name(s).", e);
      }
      for (String callSetName : callSetNames) {
        String id = nameToId.get(callSetName);
        Preconditions.checkNotNull(id,
            "Call set name '%s' does not correspond to a call set id in variant set id %s",
            callSetName, options.getVariantSetId());
        callSetIds.add(id);
      }
      return callSetIds.build().asList();
    }
  }
}
