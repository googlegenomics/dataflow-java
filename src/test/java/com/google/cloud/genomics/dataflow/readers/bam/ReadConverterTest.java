package com.google.cloud.genomics.dataflow.readers.bam;

import com.google.api.services.genomics.model.Read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import htsjdk.samtools.SAMRecord;

@RunWith(JUnit4.class)
public class ReadConverterTest {
  @Test
  public void testConversion() {
    SAMRecord record = new SAMRecord(null);
    record.setReferenceName("chr20");
    record.setAlignmentStart(1);
    record.setCigarString(String.format("%dM", 10));
    record.setMateReferenceName("chr20");
    record.setMateAlignmentStart(100);
    record.setReadPairedFlag(true);
    record.setFirstOfPairFlag(true);
    record.setMateNegativeStrandFlag(true);
    
    Read read = ReadConverter.makeRead(record);
    assertEquals((long)0, (long)read.getAlignment().getPosition().getPosition());
    assertEquals((long)1, (long)read.getAlignment().getCigar().size());
    assertEquals("chr20", read.getAlignment().getPosition().getReferenceName());
    assertEquals((int)0, (int)read.getReadNumber());
    assertEquals((long)99, (long)read.getNextMatePosition().getPosition());
    assertEquals("chr20", read.getNextMatePosition().getReferenceName());
    assertEquals((Boolean)true, read.getNextMatePosition().getReverseStrand());
  }
  
}
