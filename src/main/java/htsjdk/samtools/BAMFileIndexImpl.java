package htsjdk.samtools;

import java.util.List;

import htsjdk.samtools.seekablestream.SeekableStream;

/**
 * Dealing with visibility constraints of classes in HTSJDK we need to use.
 * Exposes BAM Index bin data we need for sharding calculations.
 */
public class BAMFileIndexImpl extends CachingBAMFileIndex {
  private SAMSequenceDictionary mBamDictionary;
  public BAMFileIndexImpl(SeekableStream stream, SAMSequenceDictionary dict) {
    super(stream, dict);
    mBamDictionary = dict;
  }
  
  public Bin getBinData(final int referenceIndex, final int binIndex) {
    final BAMIndexContent queryResults = getQueryResults(referenceIndex);

    if(queryResults == null)
        return null;
    return queryResults.getBins().getBin(binIndex);
  }
  
  public List<Chunk> getChunksOverlapping(final String reference, final int startPos, final int endPos) {
    int referenceIndex = mBamDictionary.getSequence(reference).getSequenceIndex();
    final BAMIndexContent queryResults = getQueryResults(referenceIndex);
    return queryResults.getChunksOverlapping(startPos, endPos);
  }
}
