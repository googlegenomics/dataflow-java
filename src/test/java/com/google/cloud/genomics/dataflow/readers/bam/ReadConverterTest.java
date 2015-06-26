package com.google.cloud.genomics.dataflow.readers.bam;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.genomics.gatk.common.GenomicsConverter;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

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
  @Test
  public void testByteArrayAttributes() {
    // Client code of SamRecord can pass anything to setAttribute including
    // byte[] (which doesn't have toString defined). This verifies
    // we handle that case correctly.
    SAMRecord record = new SAMRecord(null);
    record.setReferenceName("chr20");
    record.setAlignmentStart(1);
    record.setCigarString(String.format("%dM", 10));
    String s = "123456";
    record.setAttribute("FZ", s.getBytes());

    Read read = ReadConverter.makeRead(record);
    assertEquals((long)0, (long)read.getAlignment().getPosition().getPosition());
    assertEquals((long)1, (long)read.getAlignment().getCigar().size());
    assertEquals("chr20", read.getAlignment().getPosition().getReferenceName());
    assertEquals(s, read.getInfo().get("FZ").get(0));
  }

  @Test
  public void SamToReadToSamTest() throws IOException {
    String filePath = "src/test/resources/com/google/cloud/genomics/dataflow/readers/bam/conversion_test.sam";
    File samInput = new File(filePath);
    SamReader reads = SamReaderFactory.makeDefault().open(samInput);
    SAMFileHeader header = reads.getFileHeader();

    int numReads = 0;
    for (SAMRecord sam : reads){
      Read read = ReadConverter.makeRead(sam);
      SAMRecord newSam = GenomicsConverter.makeSAMRecord(read, header );
      assertEquals(newSam.getSAMString(), sam.getSAMString());
      numReads++;
    }
    assertEquals(19, numReads);//sanity check to make sure we actually read the file
  }

}
