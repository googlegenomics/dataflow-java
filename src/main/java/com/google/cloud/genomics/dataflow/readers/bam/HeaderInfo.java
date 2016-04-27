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
package com.google.cloud.genomics.dataflow.readers.bam;

import com.google.api.services.storage.Storage;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.grpc.GenomicsChannel;
import com.google.cloud.genomics.utils.grpc.ReadUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.genomics.v1.GetReadGroupSetRequest;
import com.google.genomics.v1.GetReferenceRequest;
import com.google.genomics.v1.GetReferenceSetRequest;
import com.google.genomics.v1.Read;
import com.google.genomics.v1.ReadGroup;
import com.google.genomics.v1.ReadGroupSet;
import com.google.genomics.v1.ReadServiceV1Grpc;
import com.google.genomics.v1.ReadServiceV1Grpc.ReadServiceV1BlockingStub;
import com.google.genomics.v1.Reference;
import com.google.genomics.v1.ReferenceServiceV1Grpc;
import com.google.genomics.v1.ReferenceServiceV1Grpc.ReferenceServiceV1BlockingStub;
import com.google.genomics.v1.ReferenceSet;
import com.google.genomics.v1.StreamReadsRequest;
import com.google.genomics.v1.StreamReadsResponse;
import com.google.genomics.v1.StreamingReadServiceGrpc;
import com.google.genomics.v1.StreamingReadServiceGrpc.StreamingReadServiceBlockingStub;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.ValidationStringency;
import io.grpc.Channel;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * SAM/BAM header info required for writing a BAM file.
 * Also contains the reference and start position of the first read - this can be used
 * during sharded processing to distinguish a shard that contains a first read and therefore
 * has to do some special processing (e.g. write a header into a file).
 *
 * Has methods to construct the class by reading it form the BAM file or assembling it
 * from the data behind GA4GH APIs for a given ReadGroupSet.
 */
public class HeaderInfo {
  private static final Logger LOG = Logger.getLogger(HeaderInfo.class.getName());
  public SAMFileHeader header;
  public Contig firstRead;

  public HeaderInfo(SAMFileHeader header, Contig firstShard) {
    this.header = header;
    this.firstRead = firstShard;
  }

  public boolean shardHasFirstRead(Contig shard) {
    return this.firstRead.referenceName.compareToIgnoreCase(shard.referenceName)==0 &&
        this.firstRead.start >= shard.start && this.firstRead.start <= shard.end;
  }

  static class ReferenceInfo {
    public ReferenceInfo(Reference reference, ReferenceSet referenceSet) {
      this.reference = reference;
      this.referenceSet = referenceSet;
    }
    public Reference reference;
    public ReferenceSet referenceSet;
  }

  public static HeaderInfo getHeaderFromApi(String rgsId, OfflineAuth auth, Iterable<Contig> explicitlyRequestedContigs)
      throws IOException, GeneralSecurityException {
    LOG.info("Getting metadata for header generation from ReadGroupSet: " + rgsId);

    final Channel channel = GenomicsChannel.fromOfflineAuth(auth);

    // Get readgroupset metadata and reference metadata
    ReadServiceV1BlockingStub readStub = ReadServiceV1Grpc.newBlockingStub(channel);
    GetReadGroupSetRequest getReadGroupSetRequest = GetReadGroupSetRequest
        .newBuilder()
        .setReadGroupSetId(rgsId)
        .build();

    ReadGroupSet readGroupSet = readStub.getReadGroupSet(getReadGroupSetRequest);
    String datasetId = readGroupSet.getDatasetId();
    LOG.info("Found readset " + rgsId + ", dataset " + datasetId);

    final List<ReferenceInfo> references = getReferences(channel, readGroupSet);
    List<Reference> orderedReferencesForHeader = Lists.newArrayList();
    for (ReferenceInfo ri : references) {
      orderedReferencesForHeader.add(ri.reference);
    }
    Collections.sort(orderedReferencesForHeader,
        new Comparator<Reference>() {
          @Override
          public int compare(Reference o1, Reference o2) {
            return o1.getName().compareTo(o2.getName());
          }
        });

    final SAMFileHeader fileHeader = ReadUtils.makeSAMFileHeader(readGroupSet,
        orderedReferencesForHeader);
    for (ReferenceInfo ri : references) {
      SAMSequenceRecord sr = fileHeader.getSequence(ri.reference.getName());
      sr.setAssembly(ri.referenceSet.getAssemblyId());
      sr.setSpecies(String.valueOf(ri.reference.getNcbiTaxonId()));
      sr.setAttribute(SAMSequenceRecord.URI_TAG, ri.reference.getSourceUri());
      sr.setAttribute(SAMSequenceRecord.MD5_TAG, ri.reference.getMd5Checksum());
    }

    Contig firstContig = getFirstExplicitContigOrNull(fileHeader, explicitlyRequestedContigs);
    if (firstContig == null) {
      firstContig = new Contig(fileHeader.getSequence(0).getSequenceName(), 0, 0);
      LOG.info("No explicit contig requested, using first reference " + firstContig);
    }
    LOG.info("First contig is " + firstContig);
    // Get first read
    StreamingReadServiceBlockingStub streamingReadStub =
        StreamingReadServiceGrpc.newBlockingStub(channel);
    StreamReadsRequest.Builder streamReadsRequestBuilder = StreamReadsRequest.newBuilder()
        .setReadGroupSetId(rgsId)
        .setReferenceName(firstContig.referenceName);
    if (firstContig.start != 0) {
      streamReadsRequestBuilder.setStart(Long.valueOf(firstContig.start));
    }
    if (firstContig.end != 0) {
      streamReadsRequestBuilder.setEnd(Long.valueOf(firstContig.end + 1));
    }
    final StreamReadsRequest streamReadRequest = streamReadsRequestBuilder.build();
    final Iterator<StreamReadsResponse> respIt = streamingReadStub.streamReads(streamReadRequest);
    if (!respIt.hasNext()) {
      throw new IOException("Could not get any reads for " + firstContig);
    }
    final StreamReadsResponse resp = respIt.next();
    if (resp.getAlignmentsCount() <= 0) {
      throw new IOException("Could not get any reads for " + firstContig + "(empty response)");
    }
    final Read firstRead = resp.getAlignments(0);
    final long firstReadStart = firstRead.getAlignment().getPosition().getPosition();
    LOG.info("Got first read for " + firstContig + " at position " + firstReadStart);
    final Contig firstShard = new Contig(firstContig.referenceName, firstReadStart, firstReadStart);
    return new HeaderInfo(fileHeader, firstShard);
  }

  private static List<ReferenceInfo> getReferences(Channel channel, ReadGroupSet readGroupSet) {
    Set<String> referenceSetIds = Sets.newHashSet();
    if (readGroupSet.getReferenceSetId() != null && !readGroupSet.getReferenceSetId().isEmpty()) {
      LOG.fine("Found reference set from read group set " +
          readGroupSet.getReferenceSetId());
      referenceSetIds.add(readGroupSet.getReferenceSetId());
    }
    if (readGroupSet.getReadGroupsCount() > 0) {
      LOG.fine("Found read groups");
      for (ReadGroup readGroup : readGroupSet.getReadGroupsList()) {
        if (readGroup.getReferenceSetId() != null && !readGroup.getReferenceSetId().isEmpty()) {
          LOG.fine("Found reference set from read group: " +
              readGroup.getReferenceSetId());
          referenceSetIds.add(readGroup.getReferenceSetId());
        }
      }
    }

    ReferenceServiceV1BlockingStub referenceSetStub =
        ReferenceServiceV1Grpc.newBlockingStub(channel);

    List<ReferenceInfo> references = Lists.newArrayList();
    for (String referenceSetId : referenceSetIds) {
      LOG.fine("Getting reference set " + referenceSetId);
      GetReferenceSetRequest getReferenceSetRequest = GetReferenceSetRequest
          .newBuilder().setReferenceSetId(referenceSetId).build();
      ReferenceSet referenceSet =
          referenceSetStub.getReferenceSet(getReferenceSetRequest);
      if (referenceSet == null || referenceSet.getReferenceIdsCount() == 0) {
        continue;
      }
      for (String referenceId : referenceSet.getReferenceIdsList()) {
        LOG.fine("Getting reference  " + referenceId);
        GetReferenceRequest getReferenceRequest = GetReferenceRequest
            .newBuilder().setReferenceId(referenceId).build();
        Reference reference = referenceSetStub.getReference(getReferenceRequest);
        if (reference.getName() != null && !reference.getName().isEmpty()) {
          references.add(new ReferenceInfo(reference, referenceSet));
          LOG.fine("Adding reference  " + reference.getName());
        }
      }
    }
    return references;
  }



  public static HeaderInfo getHeaderFromBAMFile(Storage.Objects storage, String BAMPath, Iterable<Contig> explicitlyRequestedContigs) throws IOException {
    HeaderInfo result = null;

    // Open and read start of BAM
    LOG.info("Reading header from " + BAMPath);
    final SamReader samReader = BAMIO
        .openBAM(storage, BAMPath, ValidationStringency.DEFAULT_STRINGENCY);
    final SAMFileHeader header = samReader.getFileHeader();
    Contig firstContig = getFirstExplicitContigOrNull(header, explicitlyRequestedContigs);
    if (firstContig == null) {
      final SAMSequenceRecord seqRecord = header.getSequence(0);
      firstContig = new Contig(seqRecord.getSequenceName(), -1, -1);
    }

    LOG.info("Reading first chunk of reads from " + BAMPath);
    final SAMRecordIterator recordIterator = samReader.query(
        firstContig.referenceName, (int)firstContig.start + 1, (int)firstContig.end + 1, false);

    Contig firstShard = null;
    while (recordIterator.hasNext() && result == null) {
      SAMRecord record = recordIterator.next();
      final int alignmentStart = record.getAlignmentStart();
      if (firstShard == null && alignmentStart > firstContig.start &&
          (alignmentStart < firstContig.end || firstContig.end == -1)) {
        firstShard = new Contig(firstContig.referenceName, alignmentStart, alignmentStart);
        LOG.info("Determined first shard to be " + firstShard);
        result = new HeaderInfo(header, firstShard);
      }
    }
    recordIterator.close();
    samReader.close();

    if (result == null) {
      throw new IOException("Did not find reads for the first contig " + firstContig.toString());
    }
    LOG.info("Finished header reading from " + BAMPath);
    return result;
  }

  /**
   * @return first contig derived from explicitly specified contigs in the options or null if none are specified.
   * The order is determined by reference lexicographic ordering and then by coordinates.
   */
  public static Contig getFirstExplicitContigOrNull(final SAMFileHeader header, Iterable<Contig> contigs) {
    if (contigs == null) {
      return null;
    }
    final ArrayList<Contig> contigsList = Lists.newArrayList(contigs);
    Collections.sort(contigsList, new Comparator<Contig>() {
      @Override
      public int compare(Contig o1, Contig o2) {
        int compRefs =  new Integer(header.getSequenceIndex(o1.referenceName)).compareTo(
            header.getSequenceIndex(o2.referenceName));
        if (compRefs != 0) {
          return compRefs;
        }
        return (int)(o1.start - o2.start);
      }
    });
    return contigsList.get(0);
  }
}



