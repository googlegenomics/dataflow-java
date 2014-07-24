package com.google.cloud.dataflow.coders;

import static org.junit.Assert.assertTrue;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

@RunWith(JUnit4.class)
public class GenericJsonCoderTest {
  
  /**
   * Tests what happens when trying to read two concatenated JSON objects using GenericJsonCoder
   */
  @Test
  public void testCodingInIterable() throws IOException {
    Read read = new Read();
    read.setId("TEST_READ_1");
    
    GenericJsonCoder<Read> coder = GenericJsonCoder.of(Read.class);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(read, output, Context.NESTED);
    read.setId("TEST_READ_2");
    coder.encode(read, output, Context.NESTED);
    
    InputStream input = new ByteArrayInputStream(output.toByteArray());
    Read out = coder.decode(input, Context.NESTED);
    assertTrue(out.getId().equals("TEST_READ_1"));
    out = coder.decode(input, Context.NESTED);
    assertTrue(out.getId().equals("TEST_READ_2"));
  }
}
