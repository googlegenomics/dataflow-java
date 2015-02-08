package htsjdk.samtools;

import java.util.List;

/**
 * Dealing with visibility constraints of classes in HTSJDK we need to use.
 * Allows construction of SAMFileSpan objects and exposes a method
 * to calculate approximate size of span in bytes.
 */
public class SAMFileSpanImpl extends BAMFileSpan {
	private static final long serialVersionUID = 1L;
	private long cachedSize = -1;
	
	public SAMFileSpanImpl() {
		super();
	}
	
	public SAMFileSpanImpl(final List<Chunk> chunks) {
		super(chunks);
	}
	
	private static final double AVERAGE_BAM_COMPRESSION_RATIO = 0.39;
	
	public long approximateSizeInBytes() {
	  if (cachedSize < 0) {
    	  cachedSize = 0;
    	  for (Chunk chunk : getChunks()) {
    	    final long chunkSpan = Math.round(((chunk.getChunkEnd()>>16)-(chunk.getChunkStart()>>16))/AVERAGE_BAM_COMPRESSION_RATIO);
            final int offsetSpan = (int)((chunk.getChunkEnd()&0xFFFF)-(chunk.getChunkStart()&0xFFFF));
            cachedSize += chunkSpan + offsetSpan;
    	  }
	  }
      return cachedSize;
	}
}
