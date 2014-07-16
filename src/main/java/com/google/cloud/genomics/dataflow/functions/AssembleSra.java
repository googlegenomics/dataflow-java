/*
 * Copyright (C) 2014 Google Inc.
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
package com.google.cloud.genomics.dataflow.functions;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.utils.GcsFileUtil;
import com.google.cloud.genomics.dataflow.utils.GenomicsException;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Given a SRA accession, downloads the SRA file, converts to paied end FASTQ, and performs assembly
 * Completed contigs are staged to GCS, either to the staging location or another specified one
 * 
 * Input: Sra Accession
 * Output: KV<Accession, Contig Location>
*/
public class AssembleSra extends DoFn<String, KV<String,String>> {
  private static final Logger LOG = Logger.getLogger(AssembleSra.class.getName());
  private static final int BUFFER_SIZE = 4096; // 4KB buffer
  
  private final String outputLocation;
  private final boolean forceAssembly;

  public AssembleSra(String outputLocation, boolean forceAssembly) {
    this.outputLocation = outputLocation;
    this.forceAssembly = forceAssembly;
  }
  
  public void setupUtils(String home) throws IOException, GenomicsException, InterruptedException {
    Process getSPAdes = null, getSraToolkit = null;
    String utilityDir = "gs://genomics-utilities";

    File SPAdes = new File(home, "SPAdes");
    if (!SPAdes.exists()) {
      LOG.info("Downloading SPAdes to " + home);
      getSPAdes = new ProcessBuilder(
          "gsutil", "-m", "cp", "-R", utilityDir + "/SPAdes", home).start();
    }
    File SraToolkit = new File(home, "SraToolkit");
    if (!SraToolkit.exists()) {
      LOG.info("Downloading SraToolkit to " + home);
      getSraToolkit = new ProcessBuilder(
          "gsutil", "-m", "cp", "-R", utilityDir + "/SraToolkit", home).start();
    }

    if (getSPAdes != null && (getSPAdes.waitFor() != 0 || getSPAdes.exitValue() != 0)) {
      LOG.severe("Error getting SPAdes utility");
      throw new GenomicsException("Error getting SPAdes utility");
    }
    if (getSraToolkit != null) {
      if (getSraToolkit.waitFor() != 0 || getSraToolkit.exitValue() != 0) {
        LOG.severe("Error getting SraToolkit utility");
        throw new GenomicsException("Error getting SraToolkit utility");
      }

      // configure SraToolkit
      Process config = new ProcessBuilder(home + "/SraToolkit/bin/vdb-config",
          "--set", "/repository/user/main/public/root=" + home).start();
      if (config.waitFor() != 0 || config.exitValue() != 0) {
        LOG.severe("Error configuring SraToolkit");
      }
      throw new GenomicsException("Error configuring SraToolkit");
    }
    
    LOG.info("Successfully set up genomics utilities");
  }

  @Override
  public void processElement(ProcessContext c) {
    String accession = c.element();
    String contigPath = outputLocation + "/contigs/" + accession + "-contigs.fasta";
    GcsUtil gcsUtil = GcsUtil.create(c.getPipelineOptions());
    if (!forceAssembly) {
      try {
        if (gcsUtil.fileSize(GcsUtil.asGcsFilename(contigPath)) != -1) {
          // Contig exists on GCS, so skip assembly and return
          c.output(KV.of(accession, contigPath));
          return;
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    // Contig doesn't exist, so go do assembly
    try {
      LOG.info("Assembling " + accession);
      String home = System.getProperty("user.home");
      setupUtils(home);
      
      Process getSra = new ProcessBuilder(
          home + "/SraToolkit/bin/prefetch", "-t", "http", accession).start();
      if (getSra.waitFor() != 0 || getSra.exitValue() != 0) {
        LOG.severe("Error fetching SRA file " + accession);
        throw new GenomicsException("Error fetching SRA file " + accession);
      }
      
      Process toFastq = new ProcessBuilder(home + "/SraToolkit/bin/fastq-dump", 
          "--split-files", "-o", home + "/" + accession, accession).start();
      if (toFastq.waitFor() != 0 || toFastq.exitValue() != 0) {
        LOG.severe("Error generating FASTQ files");
        throw new GenomicsException("Error generating FASTQ files");
      }
      
      Process assembly = new ProcessBuilder("python", home + "/SPAdes/bin/spades.py",
          "-1", home + "/" + accession + "/" + accession + "_1.fastq",
          "-2", home + "/" + accession + "/" + accession + "_2.fastq",
          "-o", home + "/" + accession, "-t", "16").start();
      if (assembly.waitFor() != 0 || assembly.exitValue() != 0) {
        LOG.severe("Error performing assembly");
        throw new GenomicsException("Error performing assembly");
      }
      
      LOG.info("Successfully completed assembly of " + accession);
      
      GcsFileUtil.localToGcs(home + "/" + accession + "/contigs.fasta", 
          contigPath, gcsUtil, "text/plain", BUFFER_SIZE);
      
      LOG.info("Successfully uploaded contig to GCS");
      
      c.output(KV.of(accession, contigPath));
      
      // Clean up mess, don't exit if something goes wrong here
      LOG.info("Cleaning up " + accession);
      Process cleanDir = new ProcessBuilder(
          "rm", "-rf", home + "/" + accession).start();
      Process cleanSra = new ProcessBuilder(
          "rm", "-f", home + "/sra/" + accession + ".sra").start();
      if (cleanDir.waitFor() != 0 || cleanDir.exitValue() != 0
          || cleanSra.waitFor() != 0 || cleanSra.exitValue() != 0) {
        LOG.warning("Error cleaning up directories for " + accession);
      }
    } catch (IOException | GenomicsException | InterruptedException e) {
      LOG.severe("Error occurred during assembly process");
      e.printStackTrace();
    }
  }
}
