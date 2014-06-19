/*
Copyright 2014 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.google.cloud.genomics.dataflow.functions;

import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.cloud.genomics.dataflow.GenomicsApi;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class ReadsetToReadTest {

  @Test
  public void testGetReads() throws Exception {
    GenomicsApi api = new GenomicsApi(null, "AIzaSyB6DSNuXBIzt4PaB9GNchRy6nZo5PdGhPI");
    List<String> readset = new ArrayList<String>();
    readset.add("CM3QxOOOAxCDg6m_oMb-kc0B");
    SearchReadsRequest request = new SearchReadsRequest().setReadsetIds(readset);
    
    ReadsetToRead fn = new ReadsetToRead(null);
    List<Read> reads = fn.getReads(request, api);
    
    assertTrue(reads.size() == 256);
    assertTrue(reads.get(0).getOriginalBases().equals(
        "TNCGCCAGGAGGCCATTTTGACTACCACACAACCGTTGCCCCACATCCTGCTGCTGACGCATGGCGGCTGGGGACAACCGCTTTGCAAC"
        + "AGCCTGCGTATGGTCACGGGCGAAATTAAAGGCGTGACGGAAATCGCGCTAATGCCTGTCGATACGCTGGGCGAGTTTTATCAACGC"
        + "GTCGAGGCNCCTCGACGCGTTGATAAAACTCGCCCAGCGTATCGACAGGCATTAGCGCGATTTCCGTCACGCCTTTAATTTCGCCCG"
        + "TGACCATACGCAGGCTGTTGCAAAGCGGTTGTCCCCAGCCGCCATGCGTCAGCAGCAGGATGTGGGGCAACGGTTGTGTGGTAGTCA"
        + "AAATGGCCTCCTGGCGCA"));
    assertTrue(request.getPageToken().equals("EkBDaGhEVFROUmVFOVBUMEY0UTBSbk5tMWZiMDFpTFd0ak1FSWlF"
        + "bE5TVWpFeE9EZzBNekl1TVRBd01ESXlOaWdBGAI"));
  }
  
  @Test
  public void testPageEnd() throws Exception {
    ReadsetToRead fn = new ReadsetToRead(null);
    List<Read> reads = fn.getReads(null, null);
    assertTrue(reads == null);
  }
}