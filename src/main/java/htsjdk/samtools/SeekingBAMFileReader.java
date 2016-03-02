package htsjdk.samtools;

import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.CloseableIterator;

import java.io.IOException;

/**
 * BAMFileReader that supports seeing to the specified offset after reading the header, so
 * the iteration begins at this offset.
 */
public class SeekingBAMFileReader extends BAMFileReader {
  long offset;
  SeekableStream stream;
  
  public SeekingBAMFileReader(final SamInputResource resource,
      final boolean eagerDecode,
      final ValidationStringency validationStringency,
      final SAMRecordFactory factory,
      long offset)
          throws IOException {
    super(resource.data().asUnbufferedSeekableStream(), (SeekableStream)null, 
        eagerDecode, validationStringency, factory);
    this.offset = offset;
    this.stream = resource.data().asUnbufferedSeekableStream();
  }
  
  @Override
  public CloseableIterator<SAMRecord> getIterator() {
    // BGZ file pointers are of the form block/offset where the high 48 bits of the 64 bit value 
    // are block location in the file.
    long offsetFilePointer = offset << 16;
    BAMFileSpan spanStartingFromOffset = new BAMFileSpan(new Chunk(offsetFilePointer, Long.MAX_VALUE));
    return getIterator(spanStartingFromOffset);
  }
}


