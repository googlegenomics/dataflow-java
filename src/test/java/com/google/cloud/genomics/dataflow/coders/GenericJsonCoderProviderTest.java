package com.google.cloud.genomics.dataflow.coders;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;

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
    Coder<?> coder = registry.getDefaultCoder(Read.class);
    assertEquals(coder, GenericJsonCoder.of(Read.class));
  }

  @Test
  public void testGenericJsonFallbackCoderProviderFallsback() throws Exception {
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
        containsString("does not implement GenericJson or Serialized")));
    Coder<?> coder = registry.getDefaultCoder(NotGenericJsonClass.class);
  }
}

