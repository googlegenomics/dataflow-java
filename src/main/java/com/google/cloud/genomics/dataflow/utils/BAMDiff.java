/*
 * Copyright (C) 2014 Google Inc.
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
package com.google.cloud.genomics.dataflow.utils;

import com.google.cloud.genomics.utils.Contig;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMProgramRecord;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.PeekIterator;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Diffs 2 BAM files and checks that they are identical.
 * This is useful when comparing outputs of various BAM exporting mechanisms.
 * Callers can specify which differences are tolerable (e.g. unmapped reads).
 * Dumps the unexpected differences found. 
 * You can run this tool from the command line like so:
 *  java -cp target/google-genomics-dataflow*runnable.jar \
 *  com.google.cloud.genomics.dataflow.utils.BAMDiff \
 *  file1 file2
 * The default options for what to ignore are currently not passed via command line
 * but are just set in the main().
 * A lot of header comparison code has been borrowed from Picrd's CompareSAMs tool.
 */
public class BAMDiff {
  private static final Logger LOG = Logger.getLogger(BAMDiff.class.getName());
      
  public static class Options {
    public Options(String contigsToProcess,boolean ignoreUnmappedReads, boolean ignoreSequenceOrder, 
        boolean ignoreSequenceProperties, boolean ignoreFileFormatVersion, 
        boolean ignoreNullVsZeroPI, boolean throwOnError) {
      this.contigsToProcess = contigsToProcess;
      this.ignoreUnmappedReads = ignoreUnmappedReads;
      this.ignoreSequenceOrder = ignoreSequenceOrder;
      this.ignoreSequenceProperties = ignoreSequenceProperties;
      this.ignoreFileFormatVersion = ignoreFileFormatVersion;
      this.ignoreNullVsZeroPI = ignoreNullVsZeroPI;
      this.throwOnError = throwOnError;
    }
    
    String contigsToProcess;
    public boolean ignoreUnmappedReads;
    public boolean throwOnError;
    public boolean ignoreSequenceOrder;
    public boolean ignoreSequenceProperties;
    public boolean ignoreFileFormatVersion;
    public boolean ignoreNullVsZeroPI;
  }
  
  public static void main(String[] args) {  
    BAMDiff diff = new BAMDiff(args[0], args[1], new BAMDiff.Options(
        args.length >= 3 ? args[2] : null, true, true, true, true, true, false ));
    try {
      diff.runDiff();
    } catch (Exception e) {
      e.printStackTrace();
      LOG.severe(e.getMessage());
    }
  }
  
  String BAMFile1; 
  String BAMFile2; 
  Options options;
  Set<String> referencesToProcess = null;
  int processedContigs = 0;
  int processedLoci = 0;
  int processedReads = 0;
  
  public BAMDiff(String bAMFile1, String bAMFile2, Options options) {
    BAMFile1=bAMFile1;
    BAMFile2=bAMFile2;
    this.options=options;
  }
  
  public void runDiff() throws Exception {
    SamReaderFactory readerFactory =  SamReaderFactory
        .makeDefault()
        .validationStringency(ValidationStringency.SILENT)
        .enable(SamReaderFactory.Option.CACHE_FILE_BASED_INDEXES);
    LOG.info("Opening file 1 for diff: " + BAMFile1);
    SamReader reader1 = readerFactory.open(new File(BAMFile1));
    LOG.info("Opening file 2 for diff: " + BAMFile2);
    SamReader reader2 = readerFactory.open(new File(BAMFile2));
    
      
    try {
      Iterator<Contig> contigsToProcess = null;
      if (options.contigsToProcess != null && !options.contigsToProcess.isEmpty()) {
        Iterable<Contig> parsedContigs = Contig.parseContigsFromCommandLine(options.contigsToProcess);
        referencesToProcess = Sets.newHashSet();
        for (Contig c : parsedContigs) {
          referencesToProcess.add(c.referenceName);
        }
        contigsToProcess = parsedContigs.iterator();
        if (!contigsToProcess.hasNext()) {
          return;
        }
      }
      LOG.info("Comparing headers");
      if (!compareHeaders(reader1.getFileHeader(), reader2.getFileHeader())) {
        error("Headers are not equal");
        return;
      }
      LOG.info("Headers are equal");
      do {
        SAMRecordIterator it1;
        SAMRecordIterator it2;
        if (contigsToProcess == null) {
          LOG.info("Checking all the reads");
          it1 = reader1.iterator();
          it2 = reader2.iterator();
        } else {
          Contig contig = contigsToProcess.next();
          LOG.info("Checking contig " + contig.toString());
          processedContigs++;
          it1 = reader1.queryOverlapping(contig.referenceName, (int)contig.start,  (int)contig.end);
          it2 = reader2.queryOverlapping(contig.referenceName, (int)contig.start,  (int)contig.end);
        }
        
        if (!compareRecords(it1, it2)) {
          break;
        }
       
        it1.close();
        it2.close();
      } while (contigsToProcess != null && contigsToProcess.hasNext());
    } catch (Exception ex) {
      throw ex;
    } finally {
      reader1.close();
      reader2.close();
    }
    LOG.info("Processed " + processedContigs + " contigs, " + 
        processedLoci + " loci, " + processedReads + " reads.");
  }
  
  class SameCoordReadSet {
    public Map<String, SAMRecord> map;
    public int coord;
    public String reference;
  }
  
  boolean compareRecords(SAMRecordIterator it1, SAMRecordIterator it2) throws Exception {
    PeekIterator<SAMRecord> pit1 = new PeekIterator<SAMRecord>(it1);
    PeekIterator<SAMRecord> pit2 = new PeekIterator<SAMRecord>(it2);
    
    do {
      SameCoordReadSet reads1 = getSameCoordReads(pit1, BAMFile1);
      SameCoordReadSet reads2 = getSameCoordReads(pit2, BAMFile2);
    
      if (reads1 == null) {
        if (reads2 == null) {
          return true;
        } else {
          error(BAMFile1 + " reads exhausted but there are still reads at " + 
              reads2.reference + ":" + reads2.coord + " in " + BAMFile2);
          return false;
        }
      } else {
        if (reads2 == null) {
          error(BAMFile2 + " reads exhausted but there are still reads at " + 
              reads1.reference + ":" + reads1.coord + " in " + BAMFile1);
          return false;
        } else {
          processedLoci++;
          if (!compareSameCoordReads(reads1, reads2)) {
            return false;
          }
          LOG.fine("Same reads at " + reads1.reference + ":" + reads1.coord);
        }
      }
      if (processedLoci % 100000000 == 0) {
        LOG.info("Working..., processed " +
            processedLoci + " loci, " + processedReads + " reads.");
      }
    } while(true);
  }
  
  SameCoordReadSet getSameCoordReads(PeekIterator<SAMRecord> it, String fileName) throws Exception {
    SameCoordReadSet ret = null;
    try {
      SAMRecord record;
      while (it.hasNext()) {
        record = it.peek();
        if (record.isSecondaryOrSupplementary() || 
            (options.ignoreUnmappedReads && record.getReadUnmappedFlag())) {
          it.next();
          continue;
        }
        if (ret != null) {
          if (record.getAlignmentStart() != ret.coord || !record.getReferenceName().equals(ret.reference)) {
            break;
          }
        } else {  
          ret = new SameCoordReadSet();
          ret.map = Maps.newHashMap();
          ret.coord = record.getAlignmentStart();
          ret.reference = record.getReferenceName();
        }
        ret.map.put(record.getReadName(), record);
        it.next();
      }
    } catch (Exception ex) {
      throw new Exception("Error reading from " + fileName + "\n" + ex.getMessage());
    }
    return ret;
  }
  
  boolean compareSameCoordReads(SameCoordReadSet reads1, SameCoordReadSet reads2) throws Exception {
    if (!reads1.reference.equals(reads2.reference)) {
      error("Different references " + reads1.reference + "!=" + reads2.reference + " at " + reads1.coord);
      return false;
    }
    if (reads1.coord != reads2.coord) {
      error("Different coordinates " + reads1.coord + "!=" + reads2.coord + " at " + reads1.reference);
      return false;
    }
    for (String readName : reads1.map.keySet()) {
      processedReads++;
      SAMRecord sr1 = reads1.map.get(readName);
      SAMRecord sr2 = reads2.map.get(readName);
      if (sr2 == null) {
        error("Read " + readName + " not found at " + reads1.reference + ":" + reads1.coord + 
            " in " + BAMFile2);
        return false;
      }
      String str1 = sr1.getSAMString();
      String str2 = sr2.getSAMString();
      if (!str1.equals(str2)) {
        error("Records are not equal for read " + readName + 
            " at " + reads1.reference + ":" + reads1.coord + "\n" + str1 + "\n" + str2);
      }
    }
    for (String readName : reads2.map.keySet()) {
      if (reads1.map.get(readName) == null) {
        error("Read " + readName + " not found at " + reads2.reference + ":" + reads2.coord + 
            " in " + BAMFile1);
        return false;
      }
    }
    return true;
  }
  
  void error(String msg) throws Exception {
    LOG.severe(msg);
    if (options.throwOnError) {
      throw new Exception(msg);
    }
  }
  
  private boolean compareHeaders(SAMFileHeader h1, SAMFileHeader h2) throws Exception {
    boolean ret = true;
    if (!options.ignoreFileFormatVersion) {
      ret = compareValues(h1.getVersion(), h2.getVersion(), "File format version") && ret;
    }
    ret = compareValues(h1.getCreator(), h2.getCreator(), "File creator") && ret;
    ret = compareValues(h1.getAttribute("SO"), h2.getAttribute("SO"), "Sort order") && ret;
    if (!compareSequenceDictionaries(h1, h2)) {
      return false;
    }
    ret = compareReadGroups(h1, h2) && ret;
    ret = compareProgramRecords(h1, h2) && ret;
    return ret;
  }

  private boolean compareProgramRecords(final SAMFileHeader h1, final SAMFileHeader h2) throws Exception {
    final List<SAMProgramRecord> l1 = h1.getProgramRecords();
    final List<SAMProgramRecord> l2 = h2.getProgramRecords();
    if (!compareValues(l1.size(), l2.size(), "Number of program records")) {
        return false;
    }
    boolean ret = true;
    for (SAMProgramRecord pr1 : l1) {
      for (SAMProgramRecord pr2 : l2) {
        if (pr1.getId().equals(pr2.getId())) {
          ret = compareProgramRecord(pr1, pr2) && ret;
        }
      }
    }
  
    return ret;
  }

  private boolean compareProgramRecord(final SAMProgramRecord programRecord1, final SAMProgramRecord programRecord2) throws Exception {
    if (programRecord1 == null && programRecord2 == null) {
        return true;
    }
    if (programRecord1 == null) {
        reportDifference("null", programRecord2.getProgramGroupId(), "Program Record");
        return false;
    }
    if (programRecord2 == null) {
        reportDifference(programRecord1.getProgramGroupId(), "null", "Program Record");
        return false;
    }
    boolean ret = compareValues(programRecord1.getProgramGroupId(), programRecord2.getProgramGroupId(),
            "Program Name");
    final String[] attributes = {"VN", "CL"};
    for (final String attribute : attributes) {
        ret = compareValues(programRecord1.getAttribute(attribute), programRecord2.getAttribute(attribute),
                attribute + " Program Record attribute") && ret;
    }
    return ret;
  }

  private boolean compareReadGroups(final SAMFileHeader h1, final SAMFileHeader h2) throws Exception {
    final List<SAMReadGroupRecord> l1 = h1.getReadGroups();
    final List<SAMReadGroupRecord> l2 = h2.getReadGroups();
    if (!compareValues(l1.size(), l2.size(), "Number of read groups")) {
        return false;
    }
    boolean ret = true;
    for (int i = 0; i < l1.size(); ++i) {
        ret = compareReadGroup(l1.get(i), l2.get(i)) && ret;
    }
    return ret;
  }

  private boolean compareReadGroup(final SAMReadGroupRecord samReadGroupRecord1, final SAMReadGroupRecord samReadGroupRecord2) throws Exception {
    boolean ret = compareValues(samReadGroupRecord1.getReadGroupId(), samReadGroupRecord2.getReadGroupId(),
            "Read Group ID");
    ret = compareValues(samReadGroupRecord1.getSample(), samReadGroupRecord2.getSample(),
            "Sample for read group " + samReadGroupRecord1.getReadGroupId()) && ret;
    ret = compareValues(samReadGroupRecord1.getLibrary(), samReadGroupRecord2.getLibrary(),
            "Library for read group " + samReadGroupRecord1.getReadGroupId()) && ret;
    final String[] attributes = {"DS", "PU", "PI", "CN", "DT", "PL"};
    for (final String attribute : attributes) {
        String a1 = samReadGroupRecord1.getAttribute(attribute);
        String a2 = samReadGroupRecord2.getAttribute(attribute);
        if (options.ignoreNullVsZeroPI && attribute.equals("PI")) {
          if (a1 == null) {
            a1 = "0";
          }
          if (a2 == null) {
            a2 = "0";
          }
        }
        ret = compareValues(a1, a2,
                attribute + " for read group " + samReadGroupRecord1.getReadGroupId()) && ret;
    }
    return ret;
  }

  private boolean compareSequenceDictionaries(final SAMFileHeader h1, final SAMFileHeader h2) throws Exception {
    final List<SAMSequenceRecord> s1 = h1.getSequenceDictionary().getSequences();
    final List<SAMSequenceRecord> s2 = h2.getSequenceDictionary().getSequences();
    if (referencesToProcess == null && s1.size() != s2.size()) {
        reportDifference(s1.size(), s2.size(), "Length of sequence dictionaries");
        return false;
    }
    boolean ret = true;
    if (referencesToProcess == null) {
      LOG.info("Comparing all sequences in the headers");
      for (int i = 0; i < s1.size(); ++i) {
        LOG.info("Comparing reference at index " + i);
        SAMSequenceRecord sr1 = s1.get(i);
        SAMSequenceRecord sr2 = options.ignoreSequenceOrder ?
            h2.getSequenceDictionary().getSequence(sr1.getSequenceName()) : 
            s2.get(i);
        if (sr2 == null) {
          error("Failed to find sequence " + sr1.getSequenceName() + " in " + BAMFile2);
        }
        ret = compareSequenceRecord(sr1, sr2, i + 1) && ret;
      }
    } else {
      LOG.info("Comparing specified sequences in the headers");
      for (String r : referencesToProcess) {
        LOG.info("Comparing reference " + r);
        ret = compareSequenceRecord(h1.getSequenceDictionary().getSequence(r),
            h2.getSequenceDictionary().getSequence(r), -1) && ret;
      }
    }
    return ret;
  }

  private boolean compareSequenceRecord(final SAMSequenceRecord sequenceRecord1, final SAMSequenceRecord sequenceRecord2, final int which) throws Exception {
    if (!sequenceRecord1.getSequenceName().equals(sequenceRecord2.getSequenceName())) {
        reportDifference(sequenceRecord1.getSequenceName(), sequenceRecord2.getSequenceName(),
                "Name of sequence record " + which);
        return false;
    }
    boolean ret = compareValues(sequenceRecord1.getSequenceLength(), sequenceRecord2.getSequenceLength(), "Length of sequence " +
            sequenceRecord1.getSequenceName());
    if (!options.ignoreSequenceProperties) {
      ret = compareValues(sequenceRecord1.getSpecies(), sequenceRecord2.getSpecies(), "Species of sequence " +
              sequenceRecord1.getSequenceName()) && ret;
      ret = compareValues(sequenceRecord1.getAssembly(), sequenceRecord2.getAssembly(), "Assembly of sequence " +
              sequenceRecord1.getSequenceName()) && ret;
      ret = compareValues(sequenceRecord1.getAttribute("M5"), sequenceRecord2.getAttribute("M5"), "MD5 of sequence " +
              sequenceRecord1.getSequenceName()) && ret;
      ret = compareValues(sequenceRecord1.getAttribute("UR"), sequenceRecord2.getAttribute("UR"), "URI of sequence " +
              sequenceRecord1.getSequenceName()) && ret;
    }
    return ret;
  }

  private <T> boolean compareValues(final T v1, final T v2, final String label) throws Exception {
    if (v1 == null) {
        if (v2 == null) {
            return true;
        }
        reportDifference(v1, v2, label);
        return false;
    }
    if (v2 == null) {
        reportDifference(v1, v2, label);
        return false;
    }
    if (!v1.equals(v2)) {
        reportDifference(v1, v2, label);
        return false;
    }
    return true;
  }

  private void reportDifference(final String s1, final String s2, final String label) throws Exception {
    error(label + " differs.\n" +
        BAMFile1 + ": " + s1 + "\n" + 
        BAMFile2 + ": " + s2);
  }

  private void reportDifference(Object o1, Object o2, final String label) throws Exception {
    if (o1 == null) {
        o1 = "null";
    }
    if (o2 == null) {
        o2 = "null";
    }
    reportDifference(o1.toString(), o2.toString(), label);
  }
}

