/*
 * Copyright (C) 2015 Google Inc.
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
package com.google.cloud.genomics.dataflow.pipelines;

import static com.google.common.collect.Lists.newArrayList;

import com.google.api.services.genomics.model.CigarUnit;
import com.google.api.services.genomics.model.LinearAlignment;
import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.First;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.readers.ReadReader;
import com.google.cloud.genomics.dataflow.readers.bam.ReadBAMTransform;
import com.google.cloud.genomics.dataflow.readers.bam.Reader;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.Paginator;
import com.google.cloud.genomics.utils.Paginator.ShardBoundary;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.codehaus.jackson.annotate.JsonIgnore;

import htsjdk.samtools.util.StringUtil;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * Simple read counting pipeline, intended as an example for reading data from 
 * APIs OR BAM files and invoking GATK tools.
 */
public class AlignmentsReads {
  private static final Logger LOG = Logger.getLogger(AlignmentsReads.class.getName());
  private static AlignReadsOptions options;
  private static Pipeline p;
  private static GenomicsFactory.OfflineAuth auth;
  public static interface AlignReadsOptions extends GenomicsDatasetOptions, GCSOptions {
    @Description("The ID of the Google Genomics ReadGroupSet this pipeline is working with. "
        + "Default (empty) indicates all ReadGroupSets.")
    @Default.String("")
    String getReadGroupSetId();

    void setReadGroupSetId(String readGroupSetId);

    @Description("The path to the BAM file to get reads data from.")
   // @Default.String("gs://genomics-public-data/platinum-genomes/bam/NA12893_S1.bam")
    @Default.String("")
    String getBAMFilePath();

    void setBAMFilePath(String filePath);
    
    @Description("Whether to shard BAM reading")
    @Default.Boolean(true)
    boolean getShardBAMReading();

    void setShardBAMReading(boolean newValue);
    
    
    class ContigsFactory implements DefaultValueFactory<Iterable<Contig>> {
      @Override
      public Iterable<Contig> create(PipelineOptions options) {
        return Iterables.transform(Splitter.on(",").split(options.as(AlignReadsOptions.class).getReferences()),
            new Function<String, Contig>() {
              @Override
              public Contig apply(String contigString) {
                ArrayList<String> contigInfo = newArrayList(Splitter.on(":").split(contigString));
                return new Contig(contigInfo.get(0), 
                    contigInfo.size() > 1 ? 
                        Long.valueOf(contigInfo.get(1)) : 0, 
                    contigInfo.size() > 2 ?
                        Long.valueOf(contigInfo.get(2)) : -1);
              }
            });
      }
    }
    
    @Default.InstanceFactory(ContigsFactory.class)
    @JsonIgnore
    Iterable<Contig> getContigs();
    
    void setContigs(Iterable<Contig> contigs);
  }
  
  public static void main(String[] args) throws GeneralSecurityException, IOException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(AlignReadsOptions.class);
    options = PipelineOptionsFactory.fromArgs(args).withValidation().as(AlignReadsOptions.class);
    // Option validation is not yet automatic, we make an explicit call here.
    GenomicsDatasetOptions.Methods.validateOptions(options);

    auth = GCSOptions.Methods.createGCSAuth(options);
    p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);

    PCollection<Read> reads = getReads();
    //reads.apply(First.<Read>of(100))
    reads.apply(ParDo.of(new DoFn<Read, String>(){

    	/**
    	 * This is the core function that should be overridden
    	 */
    	
		@Override
		public void processElement(DoFn<Read, String>.ProcessContext c)
				throws Exception {
			Read read=c.element();
			LinearAlignment alignment=read.getAlignment();
			long offset=0; //for each alignment offset is set to zero.
			StringBuilder out=new StringBuilder();
			for (CigarUnit cigar: alignment.getCigar()){
				
				switch (cigar.getOperation()){
				case "ALIGNMENT_MATCH":
				case "SEQUENCE_MATCH":
				case "SEQUENCE_MISMATCH":
					int endindex=(int)(cigar.getOperationLength()+offset);
					out.append(read.getAlignedSequence().substring((int)offset, endindex));
					/*
					int outputLen=(int)(cigar.getOperationLength()+offset);
					int endindex=(int)offset+outputLen;
					*/
			        offset += cigar.getOperationLength();
			         break;
				case "CLIP_SOFT":
				case "INSERT":
					offset += cigar.getOperationLength();
					break;
				case "PAD":
					out.append(StringUtil.repeatCharNTimes('*', cigar.getOperationLength().intValue()));
					break;
				case "DELETE":
					out.append(StringUtil.repeatCharNTimes('-', cigar.getOperationLength().intValue()));
					break;
				case "SKIP":
					out.append(StringUtil.repeatCharNTimes(' ', cigar.getOperationLength().intValue()));
					break;
				}
			}
			c.output(read.getReadGroupSetId()+"\t"+
					alignment.getPosition().getReferenceName()+"\t"+
					alignment.getPosition().getPosition()+"\t"+
					(alignment.getPosition().getReverseStrand()?"true":"false")+"\t"+
					alignment.getMappingQuality()+"\t"+
					out.length());
		}
    	
    }))
    .apply(TextIO.Write.to(options.getOutput()).named("WriteOutput"));
    /*
    PCollection<Long> readCount = reads.apply(Count.<Read>globally());
    PCollection<String> readCountText = readCount.apply(ParDo.of(new DoFn<Long, String>() {
      @Override
      public void processElement(DoFn<Long, String>.ProcessContext c) throws Exception {
        c.output(String.valueOf(c.element()));
      }
    }).named("toString"));
    *
    readCountText.apply(TextIO.Write.to(options.getOutput()).named("WriteOutput"));
    */
    p.run();
  }

  private static PCollection<Read> getReads() throws IOException {
    if (!options.getBAMFilePath().isEmpty()) {
      return getReadsFromBAMFile();
    } 
    if (!options.getReadGroupSetId().isEmpty()) {
      return getReadsFromAPI();
    }
    throw new IOException("Either BAM file or ReadGroupSet must be specified");
  }

  private static PCollection<Read> getReadsFromAPI() {
    List<SearchReadsRequest> requests = getReadRequests(options);
    PCollection<SearchReadsRequest> readRequests = p.begin()
        .apply(Create.of(requests));
    PCollection<Read> reads =
        readRequests.apply(
            ParDo.of(
                new ReadReader(auth, Paginator.ShardBoundary.OVERLAPS))
                  .named(ReadReader.class.getSimpleName()));
    return reads;
  }

  private static List<SearchReadsRequest> getReadRequests(AlignReadsOptions options) {
    
    final String readGroupSetId = options.getReadGroupSetId();
    return Lists.newArrayList(Iterables.transform(
        Iterables.concat(Iterables.transform(options.getContigs(),
          new Function<Contig, Iterable<Contig>>() {
            @Override
            public Iterable<Contig> apply(Contig contig) {
              return contig.getShards();
            }
          })),
        new Function<Contig, SearchReadsRequest>() {
          @Override
          public SearchReadsRequest apply(Contig shard) {
            return shard.getReadsRequest(readGroupSetId);
          }
        }));
   }
  
  private static PCollection<Read> getReadsFromBAMFile() throws IOException {
    LOG.info("getReadsFromBAMFile");

    final Iterable<Contig> contigs = options.getContigs();
        
    if (options.getShardBAMReading()) {
      return ReadBAMTransform.getReadsFromBAMFilesSharded(p, 
          auth,
          contigs, 
          Collections.singletonList(options.getBAMFilePath()));
    } else {  // For testing and comparing sharded vs. not sharded only
      return p.apply(
          Create.of(
              Reader.readSequentiallyForTesting(
                  GCSOptions.Methods.createStorageClient(options, auth),
                  options.getBAMFilePath(),
                  contigs.iterator().next())));
    }
  }
}
