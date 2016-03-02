package htsjdk.samtools;

import java.io.OutputStream;

/**
 * This class is adapted from HTSJDK BAMIndexer 
 * See https://github.com/samtools/htsjdk/blob/master/src/java/htsjdk/samtools/BAMIndexer.java
 * and modified to support sharded index writing, where index for each reference is generated
 * separately and then the index shards are combined.
 */
public class BAMShardIndexer {
  // output written as binary, or (for debugging) as text
  private final BinaryBAMShardIndexWriter outputWriter;

  // content is built up from the input bam file using this
  private final BAMIndexBuilder indexBuilder;
  
  // Index of the reference for which the index is being written
  int reference;

  public BAMShardIndexer(OutputStream output, SAMFileHeader header, int reference) {
    indexBuilder = new BAMIndexBuilder(header.getSequenceDictionary(), reference);
    final boolean isFirstIndexShard = reference == 0;
    final int numReferencesToWriteInTheHeader = isFirstIndexShard ? 
        header.getSequenceDictionary().size() : 0;
    outputWriter = new BinaryBAMShardIndexWriter(numReferencesToWriteInTheHeader, output);
    this.reference = reference;
  }
  
  public void processAlignment(final SAMRecord rec) {
      try {
          indexBuilder.processAlignment(rec);
      } catch (final Exception e) {
          throw new SAMException("Exception creating BAM index for record " + rec, e);
      }
  }

  /**
   * Finalizes writing and closes the file.
   * @return count of records with no coordinates.
   */
  public long finish() {
    final BAMIndexContent content = indexBuilder.processReference(reference);
    outputWriter.writeReference(content);
    outputWriter.close();
    return indexBuilder.getNoCoordinateRecordCount();
  }

  /**
   * Class for constructing BAM index files.
   * One instance is used to construct an entire index.
   * processAlignment is called for each alignment until a new reference is encountered, then
   * processReference is called when all records for the reference have been processed.
   */
  private class BAMIndexBuilder {

      private final SAMSequenceDictionary sequenceDictionary;

      private BinningIndexBuilder binningIndexBuilder;

      private int currentReference = -1;

      // information in meta data
      private final BAMIndexMetaData indexStats = new BAMIndexMetaData();

      BAMIndexBuilder(final SAMSequenceDictionary sequenceDictionary, int reference) {
          this.sequenceDictionary = sequenceDictionary;
          if (!sequenceDictionary.isEmpty()) startNewReference(reference);
      }

      /**
       * Record any index information for a given BAM record
       *
       * @param rec The BAM record. Requires rec.getFileSource() is non-null.
       */
      public void processAlignment(final SAMRecord rec) {

          // metadata
          indexStats.recordMetaData(rec);

          if (rec.getAlignmentStart() == SAMRecord.NO_ALIGNMENT_START) {
              return; // do nothing for records without coordinates, but count them
          }

          // various checks
          final int reference = rec.getReferenceIndex();
          if (reference != currentReference) {
              throw new SAMException("Unexpected reference " + reference +
                      " when constructing index for " + currentReference + " for record " + rec);
          }

          binningIndexBuilder.processFeature(new BinningIndexBuilder.FeatureToBeIndexed() {
              @Override
              public int getStart() {
                  return rec.getAlignmentStart();
              }

              @Override
              public int getEnd() {
                  return rec.getAlignmentEnd();
              }

              @Override
              public Integer getIndexingBin() {
                  final Integer binNumber = rec.getIndexingBin();
                  return (binNumber == null ? rec.computeIndexingBin() : binNumber);

              }

              @Override
              public Chunk getChunk() {
                  final SAMFileSource source = rec.getFileSource();
                  if (source == null) {
                      throw new SAMException("No source (virtual file offsets); needed for indexing on BAM Record " + rec);
                  }
                  return ((BAMFileSpan) source.getFilePointer()).getSingleChunk();
              }
          });

      }

      /**
       * Creates the BAMIndexContent for this reference.
       * Requires all alignments of the reference have already been processed.
       *
       * @return Null if there are no features for this reference.
       */
      public BAMIndexContent processReference(final int reference) {

          if (reference != currentReference) {
              throw new SAMException("Unexpected reference " + reference + " when constructing index for " + currentReference);
          }

          final BinningIndexContent indexContent = binningIndexBuilder.generateIndexContent();
          if (indexContent == null) return null;
          return new BAMIndexContent(indexContent.getReferenceSequence(), indexContent.getBins(),
                  indexStats, indexContent.getLinearIndex());

      }

      /**
       * @return the count of records with no coordinate positions
       */
      public long getNoCoordinateRecordCount() {
          return indexStats.getNoCoordinateRecordCount();
      }

      /**
       * reinitialize all data structures when the reference changes
       */
      void startNewReference(int reference) {
          currentReference  = reference;
          // I'm not crazy about recycling this object, but that is the way it was originally written and
          // it helps keep track of no-coordinate read count (which shouldn't be stored in this class anyway).
          indexStats.newReference();
          binningIndexBuilder = new BinningIndexBuilder(currentReference,
                  sequenceDictionary.getSequence(currentReference).getSequenceLength());
      }
  }
}

