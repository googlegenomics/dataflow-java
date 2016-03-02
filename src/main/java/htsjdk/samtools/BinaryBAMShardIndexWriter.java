package htsjdk.samtools;

import htsjdk.samtools.util.BinaryCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Adapted from HTSJDK Binary BAMIndexWriter,
 * See https://github.com/samtools/htsjdk/blob/master/src/java/htsjdk/samtools/BinaryBAMIndexWriter.java
 * Only writes header for the first reference.
 */
public class BinaryBAMShardIndexWriter implements BAMIndexWriter {
  private final BinaryCodec codec;

  /**
   * @param nRef Number of reference sequences. If zero is passed then header is not written.
   * This is useful in sharded writing as we only want the header written for the first shard.
   * 
   * @param output BAM index output stream.  This stream will be closed when BinaryBAMIndexWriter.close() is called.
   */
  public BinaryBAMShardIndexWriter(final int nRef, final OutputStream output) {
      try {
          codec = new BinaryCodec(output);
          if (nRef > 0) {
            writeHeader(nRef);
          }
      } catch (final Exception e) {
          throw new SAMException("Exception opening output stream", e);
      }
  }

  /**
   * Write this content as binary output
   */
  @Override
  public void writeReference(final BAMIndexContent content) {

      if (content == null) {
          writeNullContent();
          return;
      }

      // write bins

      final BAMIndexContent.BinList bins = content.getBins();
      final int size = bins == null ? 0 : content.getNumberOfNonNullBins();

      if (size == 0) {
          writeNullContent();
          return;
      }

      //final List<Chunk> chunks = content.getMetaData() == null ? null
      //        : content.getMetaData().getMetaDataChunks();
      final BAMIndexMetaData metaData = content.getMetaData();

      codec.writeInt(size + ((metaData != null)? 1 : 0 ));
      // codec.writeInt(size);
      for (final Bin bin : bins) {   // note, bins will always be sorted
          if (bin.getBinNumber() == GenomicIndexUtil.MAX_BINS)
              continue;
          writeBin(bin);
      }

      // write metadata "bin" and chunks        
      if (metaData != null)
          writeChunkMetaData(metaData);

      // write linear index

      final LinearIndex linearIndex = content.getLinearIndex();
      final long[] entries = linearIndex == null ? null : linearIndex.getIndexEntries();
      final int indexStart = linearIndex == null ? 0 : linearIndex.getIndexStart();
      final int n_intv = entries == null ? indexStart : entries.length + indexStart;
      codec.writeInt(n_intv);
      if (entries == null) {
          return;
      }
      // since indexStart is usually 0, this is usually a no-op
      for (int i = 0; i < indexStart; i++) {
          codec.writeLong(0);
      }
      for (int k = 0; k < entries.length; k++) {
          codec.writeLong(entries[k]);
      }
      try {
          codec.getOutputStream().flush();
      } catch (final IOException e) {
          throw new SAMException("IOException in BinaryBAMIndexWriter reference " + content.getReferenceSequence(), e);
      }
  }

  /**
   * Writes out the count of records without coordinates
   *
   * @param count
   */
  @Override
  public void writeNoCoordinateRecordCount(final Long count) {
      codec.writeLong(count == null ? 0 : count);
  }

  /**
   * Any necessary processing at the end of the file
   */
  @Override
  public void close() {
      codec.close();
  }

  private void writeBin(final Bin bin) {
      final int binNumber = bin.getBinNumber();
      if (binNumber >= GenomicIndexUtil.MAX_BINS){
          throw new SAMException("Unexpected bin number when writing bam index " + binNumber);
      }
      
      codec.writeInt(binNumber);
      if (bin.getChunkList() == null){
          codec.writeInt(0);
          return;
      }
      final List<Chunk> chunkList = bin.getChunkList();
      final int n_chunk = chunkList.size();
      codec.writeInt(n_chunk);
      for (final Chunk c : chunkList) {
          codec.writeLong(c.getChunkStart());
          codec.writeLong(c.getChunkEnd());
      }
  }

  /**
   * Write the meta data represented by the chunkLists associated with bin MAX_BINS 37450
   *
   * @param metaData information describing numAligned records, numUnAligned, etc
   */
  private void writeChunkMetaData(final BAMIndexMetaData metaData) {
      codec.writeInt(GenomicIndexUtil.MAX_BINS);
      final int nChunk = 2;
      codec.writeInt(nChunk);
      codec.writeLong(metaData.getFirstOffset());
      codec.writeLong(metaData.getLastOffset());
      codec.writeLong(metaData.getAlignedRecordCount());
      codec.writeLong(metaData.getUnalignedRecordCount());

  }

  private void writeHeader(int nRef) {
      // magic string
      final byte[] magic = BAMFileConstants.BAM_INDEX_MAGIC;
      codec.writeBytes(magic);
      codec.writeInt(nRef);
  }

  private void writeNullContent() {
      codec.writeLong(0);  // 0 bins , 0 intv
  }
}

