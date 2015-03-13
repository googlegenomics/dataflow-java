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
package com.google.cloud.genomics.dataflow.readers.bam;

import com.google.api.services.genomics.model.CigarUnit;
import com.google.api.services.genomics.model.LinearAlignment;
import com.google.api.services.genomics.model.Position;
import com.google.api.services.genomics.model.Read;
import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;

import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.SAMException;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.SequenceUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts SAMRecords to Reads.
 */
public class ReadConverter {
  static HashBiMap<String, String> CIGAR_OPERATIONS;
  static BiMap<String, String> CIGAR_OPERATIONS_INV;

  static {
    CIGAR_OPERATIONS = HashBiMap.create();
    CIGAR_OPERATIONS.put("ALIGNMENT_MATCH", "M");
    CIGAR_OPERATIONS.put("CLIP_HARD", "H");
    CIGAR_OPERATIONS.put("CLIP_SOFT", "S");
    CIGAR_OPERATIONS.put("DELETE", "D");
    CIGAR_OPERATIONS.put("INSERT", "I");
    CIGAR_OPERATIONS.put("PAD", "P");
    CIGAR_OPERATIONS.put("SEQUENCE_MATCH", "=");
    CIGAR_OPERATIONS.put("SEQUENCE_MISMATCH", "X");
    CIGAR_OPERATIONS.put("SKIP", "N");
    CIGAR_OPERATIONS_INV = CIGAR_OPERATIONS.inverse();
  }

  /**
   * Generates a Read from a SAMRecord. 
   */
  public static final Read makeRead(final SAMRecord record) {
    Read read = new Read();
    read.setId(record.getReadName()); // TODO: make more unique
    read.setFragmentName(record.getReadName());
    read.setReadGroupId(getAttr(record, "RG"));
    read.setNumberReads(record.getReadPairedFlag() ? 1 : 2);
    read.setProperPlacement(record.getReadPairedFlag() && record.getProperPairFlag());
    if (!record.getReadUnmappedFlag() && record.getAlignmentStart() > 0) {
      LinearAlignment alignment = new LinearAlignment();

      Position position = new Position();
      position.setPosition((long) record.getAlignmentStart() - 1);
      position.setReferenceName(record.getReferenceName());
      position.setReverseStrand(record.getReadNegativeStrandFlag());
      alignment.setPosition(position);

      alignment.setMappingQuality(record.getMappingQuality());

      final String referenceSequence = (record.getAttribute("MD") != null) ? new String(
          SequenceUtil.makeReferenceFromAlignment(record, true))
          : null;
      List<CigarUnit> cigar = Lists.transform(record.getCigar().getCigarElements(),
          new Function<CigarElement, CigarUnit>() {
            @Override
            public CigarUnit apply(CigarElement c) {
              CigarUnit u = new CigarUnit();
              CigarOperator o = c.getOperator();
              u.setOperation(CIGAR_OPERATIONS_INV.get(o.toString()));
              u.setOperationLength((long) c.getLength());
              if (referenceSequence != null && (u.getOperation().equals("SEQUENCE_MISMATCH")
                  || u.getOperation().equals("DELETE"))) {
                u.setReferenceSequence(referenceSequence);
              }
              return u;
            }
          });
      alignment.setCigar(cigar);
      read.setAlignment(alignment);
    }
    read.setDuplicateFragment(record.getDuplicateReadFlag());
    read.setFragmentLength(record.getReadLength());
    if (record.getReadPairedFlag()) {
      if (record.getFirstOfPairFlag()) {
        read.setReadNumber(0);
      } else if (record.getSecondOfPairFlag()) {
        read.setReadNumber(1);
      }

      if (!record.getMateUnmappedFlag()) {
        Position matePosition = new Position();
        matePosition.setPosition((long) record.getMateAlignmentStart() - 1);
        matePosition.setReferenceName(record.getMateReferenceName());
        matePosition.setReverseStrand(record.getMateNegativeStrandFlag());
        read.setNextMatePosition(matePosition);
      }
    }
    read.setFailedVendorQualityChecks(record.getReadFailsVendorQualityCheckFlag());
    read.setSecondaryAlignment(record.getNotPrimaryAlignmentFlag());
    read.setSupplementaryAlignment(record.getSupplementaryAlignmentFlag());
    read.setAlignedSequence(record.getReadString());
    byte[] baseQualities = record.getBaseQualities();
    if (baseQualities.length > 0) {
      List<Integer> readBaseQualities = new ArrayList<Integer>(baseQualities.length);
      for (byte b : baseQualities) {
        readBaseQualities.add(new Integer(b));
      }
      read.setAlignedQuality(readBaseQualities);
    }

    return read;
  }

  public static String getAttr(SAMRecord record, String attributeName) {
    try {
      return record.getStringAttribute(attributeName);
    } catch (SAMException ex) {
      return "";
    }
  }
}
