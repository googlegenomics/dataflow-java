package com.google.cloud.genomics.dataflow.coders;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.Proto2Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.Serializable;

public class GenericJsonCoderProviderTest {
  private static CoderRegistry registry;

  private static class NotGenericJsonClass {}

  private static class SerializableClass implements Serializable {}

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    registry = new CoderRegistry();
    registry.registerStandardCoders();
    registry.setFallbackCoderProvider(GenericJsonCoder.PROVIDER);
  }

  @Test
  public void testGenericJsonFallbackCoderProvider() throws Exception {
    Coder<?> coder = registry.getDefaultCoder(com.google.api.services.genomics.model.Read.class);
    assertEquals(coder, GenericJsonCoder.of(com.google.api.services.genomics.model.Read.class));
  }

  @Test
  public void testGenericJsonFallbackCoderProviderFallsbackToProto2Coder() throws Exception {
    Coder<?> coder = registry.getDefaultCoder(com.google.genomics.v1.Read.class);
    assertEquals(coder, Proto2Coder.of(com.google.genomics.v1.Read.class));
  }

  @Test
  public void testGenericJsonFallbackCoderProviderFallsbackToSerialiableCoder() throws Exception {
    Coder<?> coder = registry.getDefaultCoder(SerializableClass.class);
    assertEquals(coder, SerializableCoder.of(SerializableClass.class));
  }

  @Test
  public void testGenericJsonFallbackCoderProviderThrows() throws Exception {
    thrown.expect(CannotProvideCoderException.class);
    thrown.expectMessage(allOf(
        containsString(NotGenericJsonClass.class.getCanonicalName()),
        containsString("No CoderFactory has been registered"),
        containsString("does not have a @DefaultCoder annotation"),
        containsString("does not implement GenericJson, Message, or Serializable")));
    Coder<?> coder = registry.getDefaultCoder(NotGenericJsonClass.class);
  }
}
