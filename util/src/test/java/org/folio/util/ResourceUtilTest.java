package org.folio.util;

import static org.junit.Assert.*;

import java.io.IOException;

import org.folio.rest.testing.UtilityClassTester;
import org.junit.Test;

public class ResourceUtilTest {
  @Test
  public void utilityClass() {
    UtilityClassTester.assertUtilityClass(ResourceUtil.class);
  }

  @Test(expected = NullPointerException.class)
  public void nullFilename() throws IOException {
    ResourceUtil.asString(null);
  }

  @Test(expected = IOException.class)
  public void fileDoesNotExist() throws IOException {
    ResourceUtil.asString("foobar");
  }

  @Test
  public void readEmptyFile() throws IOException {
    assertEquals("", ResourceUtil.asString("ResourceUtilEmpty"));
  }

  @Test
  public void readExampleFile() throws IOException {
    assertEquals("first line\numlauts: äöü\n", ResourceUtil.asString("ResourceUtilExample"));
  }

  @Test
  public void read3000() throws IOException {
    char [] expected = new char [3000];
    for (int i=0; i<expected.length; i++) {
      expected[i] = 'a';
    }
    assertEquals(new String(expected), ResourceUtil.asString("ResourceUtil3000"));
  }
}
