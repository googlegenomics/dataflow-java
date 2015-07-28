/*
 * Copyright 2015 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.genomics.dataflow.utils;

import com.google.api.services.genomics.model.Position;
import com.google.api.services.genomics.model.ReadGroupSet;
import com.google.api.services.genomics.model.SearchReadGroupSetsRequest;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.genomics.dataflow.model.ReadBaseQuality;
import com.google.cloud.genomics.dataflow.model.ReadBaseWithReference;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.Paginator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.genomics.v1.CigarUnit;
import com.google.genomics.v1.Read;
import java.io.IOException;
import java.security.GeneralSecurityException;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods for working with genetic read data.
 */
public class ReadUtils {

  private static final String UNINITIALIZED_REFERENCE_SEQUENCE = "UNINITIALIZED";

  /**
   * Use the given read to build a list of aligned read and reference base
   * information.
   *
   * @param read The read with the alignment information
   * @return read and reference information.  For a read without an alignment
   *            or cigar units, null is returned.
   */
  public static List<ReadBaseWithReference> extractReadBases(Read read) {

    // Make sure this read has a valid alignment with Cigar Units
    if (!read.hasAlignment() || (read.getAlignment().getCigarCount() == 0)) {
      return null;
    }

    ImmutableList.Builder<ReadBaseWithReference> bases = ImmutableList.builder();

    String readSeq = read.getAlignedSequence();
    List<Integer> readQual = read.getAlignedQualityList();
    String refSeq = UNINITIALIZED_REFERENCE_SEQUENCE;

    int refPosAbsoluteOffset = 0;
    int readOffset = 0;
    for (CigarUnit unit : read.getAlignment().getCigarList()) {
      switch(unit.getOperation()) {
        case ALIGNMENT_MATCH:
        case SEQUENCE_MISMATCH:
        case SEQUENCE_MATCH:
          for (int i = 0; i < unit.getOperationLength(); i++) {
            String refBase = "";
            if (unit.getOperation().equals(CigarUnit.Operation.SEQUENCE_MATCH)) {
              refBase = readSeq.substring(readOffset, readOffset + 1);
            } else if (!unit.getReferenceSequence().isEmpty()) {
              // try to get the ref sequence from the Cigar unit
              refBase = unit.getReferenceSequence().substring(i, i + 1);
            } else {
              // try to get the ref sequence by fully parsing the MD tag if not already cached
              if (refSeq != null && refSeq.equals(UNINITIALIZED_REFERENCE_SEQUENCE)) {
                refSeq = com.google.cloud.genomics.grpc.ReadUtils
                    .inferReferenceSequenceByParsingMdFlag(read);
              }
              if (refSeq != null) {
                refBase = refSeq.substring(readOffset, readOffset + 1);
              }
            }
            String name = read.getAlignment().getPosition().getReferenceName();
            Matcher m = Pattern.compile("^(chr)?(X|Y|([12]?\\d))$").matcher(name);
            if (m.matches()) {
              name = m.group(m.groupCount() - 1);
            }
            Position refPosition = new Position()
                .setReferenceName(name)
                .setPosition(read.getAlignment().getPosition().getPosition()
                  + refPosAbsoluteOffset);
            bases.add(new ReadBaseWithReference(new ReadBaseQuality(
                readSeq.substring(readOffset, readOffset + 1),
                readQual.get(readOffset)),
                refBase,
                refPosition));
            refPosAbsoluteOffset++;
            readOffset++;
          }
          break;

        case PAD:        // padding (silent deletion from padded reference)
        case CLIP_HARD:  // hard clipping (clipped sequences NOT present in seq)
          break;

        case CLIP_SOFT:  // soft clipping (clipped sequences present in SEQ)
        case INSERT:     // insertion to the reference
          readOffset += unit.getOperationLength();
          break;

        case DELETE:  // deletion from the reference
        case SKIP:    // intron (mRNA-to-genome alignment only)
          refPosAbsoluteOffset += unit.getOperationLength();
          break;

        default:
          throw new IllegalArgumentException("Illegal cigar code: " + unit.getOperation());
      }
    }

    return bases.build();
  }

  /**
   * Make sure a read is from a chromosome.
   */
  public static final SerializableFunction<Read, Boolean> IS_ON_CHROMOSOME
      = new SerializableFunction<Read, Boolean>() {
        @Override
        public Boolean apply(Read r) {
          return Pattern.compile("^(chr)?(X|Y|([12]?\\d))$")
              .matcher(r.getAlignment().getPosition().getReferenceName())
              .matches();
        }
      };

  /**
   * Make sure a read passed QC.
   */
  public static final SerializableFunction<Read, Boolean> IS_NOT_QC_FAILURE
      = new SerializableFunction<Read, Boolean>() {
        @Override
        public Boolean apply(Read r) {
          return !r.getFailedVendorQualityChecks();
        }
      };

  /**
   * Make sure a read is not a duplicate.
   */
  public static final SerializableFunction<Read, Boolean> IS_NOT_DUPLICATE
      = new SerializableFunction<Read, Boolean>() {
        @Override
        public Boolean apply(Read r) {
          return !r.getDuplicateFragment();
        }
      };

  /**
   * Make sure the read alignment is proper (paired end reads facing each other with acceptable
   * fragment size).
   */
  public static final SerializableFunction<Read, Boolean> IS_PROPER_PLACEMENT
      = new SerializableFunction<Read, Boolean>() {
        @Override
        public Boolean apply(Read r) {
          return r.getProperPlacement();
        }
      };

  /**
   * Gets ReadGroupSetIds from a given datasetId using the Genomics API.
   */
  public static List<String> getReadGroupSetIds(String datasetId, GenomicsFactory.OfflineAuth auth)
      throws IOException, GeneralSecurityException {
    List<String> output = Lists.newArrayList();
    Iterable<ReadGroupSet> rgs = Paginator.ReadGroupSets.create(
        auth.getGenomics(auth.getDefaultFactory()))
        .search(new SearchReadGroupSetsRequest().setDatasetIds(Lists.newArrayList(datasetId)),
            "readGroupSets/id,nextPageToken");
    for (ReadGroupSet r : rgs) {
      output.add(r.getId());
    }
    if (output.isEmpty()) {
      throw new IOException("Dataset " + datasetId + " does not contain any ReadGroupSets");
    }
    return output;
  }
}
