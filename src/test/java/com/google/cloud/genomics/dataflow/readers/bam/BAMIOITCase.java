package com.google.cloud.genomics.dataflow.readers.bam;

import com.google.api.services.genomics.model.Read;
import com.google.api.services.storage.Storage;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.gatk.common.GenomicsConverter;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class BAMIOITCase {
  final String API_KEY = System.getenv("GOOGLE_API_KEY");  
  final String TEST_BAM_FNAME = "gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/pilot_data/data/NA06985/alignment/NA06985.454.MOSAIK.SRP000033.2009_11.bam";
  final int EXPECTED_UNMAPPED_READS_COUNT = 685;
  
  @Before
  public void voidEnsureEnvVar() {
	  Assert.assertNotNull("You must set the GOOGLE_API_KEY environment variable for this test.", API_KEY);
  }    
	  
  @Test
  public void openBAMTest() throws IOException {
	  GCSOptions popts = PipelineOptionsFactory.create().as(GCSOptions.class);
	  popts.setApiKey(API_KEY);
	  final Storage.Objects storageClient = Transport.newStorageClient(popts).build().objects();

	  SamReader samReader = BAMIO.openBAM(storageClient, TEST_BAM_FNAME, ValidationStringency.DEFAULT_STRINGENCY);	
	  SAMRecordIterator iterator =  samReader.query("1", 550000, 560000, false);
	  int readCount = 0;
	  while (iterator.hasNext()) {
	      iterator.next();
	      readCount++;
	  }
	  Assert.assertEquals("Unexpected count of unmapped reads",
			  EXPECTED_UNMAPPED_READS_COUNT, readCount);
  }
}
