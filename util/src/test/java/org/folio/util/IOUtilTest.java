package org.folio.util;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.folio.rest.testing.UtilityClassTester;
import org.junit.Test;

public class IOUtilTest {
  @Test
  public void utilityClass() {
    UtilityClassTester.assertUtilityClass(IOUtil.class);
  }

  @Test(expected = NullPointerException.class)
  public void nullFilename() throws IOException {
    IOUtil.toUTF8String(null);
  }

  private InputStream inputStream(String s) {
    return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void readEmptyStream() throws IOException {
    assertEquals("", IOUtil.toUTF8String(inputStream("")));
  }

  @Test
  public void readUmlauts() throws IOException {
    String s = "first line\numlauts: äöü\n";
    assertEquals(s, IOUtil.toUTF8String(inputStream(s)));
  }

  @Test
  public void readUmlautsWrongEncoding() throws IOException {
    String s = "first line\numlauts: äöü\n";
    assertNotEquals(s, IOUtil.toUTF8String(inputStream(s)), StandardCharsets.ISO_8859_1);
  }

  @Test
  public void read3000() throws IOException {
    char [] expected = new char [3000];
    for (int i=0; i<expected.length; i++) {
      expected[i] = 'a';
    }
    String s = new String(expected);
    assertEquals(s, IOUtil.toUTF8String(inputStream(s)));
  }
}
