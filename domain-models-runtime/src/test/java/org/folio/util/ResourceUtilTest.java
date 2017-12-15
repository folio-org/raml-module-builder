package org.folio.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

/**
 * This tests whether ResourceUtil.java in the org.folio:util package
 * correctly loads from domain-models-runtime.jar.
 */
public class ResourceUtilTest {
  @Test
  public void readFromUtilJar() throws IOException {
    assertEquals("domain-models-runtime", ResourceUtil.asString("resourceUtil.txt"));
  }

  @Test
  public void readFromUtilJarWithClass() throws IOException {
    assertEquals("domain-models-runtime", ResourceUtil.asString("resourceUtil.txt", ResourceUtilTest.class));
  }
}
