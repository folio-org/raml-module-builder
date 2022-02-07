package org.folio.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.folio.okapi.testing.UtilityClassTester;
import org.junit.jupiter.api.Test;

class IoUtilTest {
  private static final String example = "first line\numlauts: äöü\n";

  @Test
  void utilityClass() {
    UtilityClassTester.assertUtilityClass(IoUtil.class);
  }

  @Test
  void nullFilename() throws IOException {
    assertThrows(NullPointerException.class, () -> IoUtil.toStringUtf8((InputStream) null));
  }

  private InputStream inputStream(String s) {
    return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void readEmptyStream() throws IOException {
    assertThat(IoUtil.toStringUtf8(inputStream("")), is(""));
  }

  @Test
  void readUmlauts() throws IOException {
    assertThat(IoUtil.toStringUtf8(inputStream(example)), is(example));
  }

  @Test
  void readUmlautsEncoding() throws IOException {
    assertThat(IoUtil.toString(inputStream(example), StandardCharsets.UTF_8), is(example));
    assertThat(IoUtil.toString(inputStream(example), StandardCharsets.ISO_8859_1), is(not(example)));
  }

  @Test
  void read3000() throws IOException {
    char [] expected = new char [3000];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = 'a';
    }
    String s = new String(expected);
    assertThat(IoUtil.toStringUtf8(inputStream(s)), is(s));
  }

  @Test
  void readFile() throws IOException {
    String path = System.getProperty("user.dir") + "/src/test/resources/ResourceUtilExample.bin";
    assertThat(IoUtil.toStringUtf8(path), is(example));
  }
}
