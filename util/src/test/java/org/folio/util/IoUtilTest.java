package org.folio.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.folio.rest.testing.UtilityClassTester;
import org.junit.Test;

public class IoUtilTest {
  private static final String example = "first line\numlauts: äöü\n";

  @Test
  public void utilityClass() {
    UtilityClassTester.assertUtilityClass(IoUtil.class);
  }

  @Test(expected = NullPointerException.class)
  public void nullFilename() throws IOException {
    IoUtil.toStringUtf8((InputStream)null);
  }

  private InputStream inputStream(String s) {
    return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void readEmptyStream() throws IOException {
    assertEquals("", IoUtil.toStringUtf8(inputStream("")));
  }

  @Test
  public void readUmlauts() throws IOException {
    assertEquals(example, IoUtil.toStringUtf8(inputStream(example)));
  }

  @Test
  public void readUmlautsEncoding() throws IOException {
    assertEquals(example, IoUtil.toString(inputStream(example), StandardCharsets.UTF_8));
    assertNotEquals(example, IoUtil.toString(inputStream(example), StandardCharsets.ISO_8859_1));
  }

  @Test
  public void read3000() throws IOException {
    char [] expected = new char [3000];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = 'a';
    }
    String s = new String(expected);
    assertEquals(s, IoUtil.toStringUtf8(inputStream(s)));
  }

  @Test
  public void readFile() throws IOException {
    String path = System.getProperty("user.dir") + "/src/test/resources/ResourceUtilExample.bin";
    assertEquals(example, IoUtil.toStringUtf8(path));
  }
}
