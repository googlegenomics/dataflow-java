package com.google.cloud.genomics.dataflow.functions;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.genomics.dataflow.readers.bam.HeaderInfo;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;

import java.util.logging.Logger;

public class GetReferencesFromHeaderFn extends DoFn<HeaderInfo, String> {
  private static final Logger LOG = Logger.getLogger(GetReferencesFromHeaderFn.class.getName());

  @Override
  public void processElement(DoFn<HeaderInfo, String>.ProcessContext c) throws Exception {
    final SAMFileHeader header = c.element().header;
    for (SAMSequenceRecord sequence : header.getSequenceDictionary().getSequences()) {
      c.output(sequence.getSequenceName());
    }
    LOG.info("Processed " + header.getSequenceDictionary().size() + " references");
  }
}

