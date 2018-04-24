package org.folio.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;

import org.folio.rest.testing.UtilityClassTester;
import org.junit.jupiter.api.Test;

class ResourceUtilTest {
  @Test
  void utilityClass() {
    UtilityClassTester.assertUtilityClass(ResourceUtil.class);
  }

  @Test
  void nullFilename() throws IOException {
    assertThrows(NullPointerException.class, () -> ResourceUtil.asString(null));
  }

  @Test
  void fileDoesNotExist() {
    assertThrows(IOException.class, () -> ResourceUtil.asString("foobar"));
  }

  @Test
  void readEmptyFile() throws IOException {
    assertThat(ResourceUtil.asString("ResourceUtilEmpty.bin"), is(""));
  }

  @Test
  void readEmptyFileFromOtherClass() throws IOException {
    assertThat(ResourceUtil.asString("ResourceUtilEmpty.bin", File.class), is(""));
  }

  @Test
  void readExampleFile() throws IOException {
    assertThat(ResourceUtil.asString("ResourceUtilExample.bin"), is("first line\numlauts: äöü\n"));
  }

  @Test
  void read3000() throws IOException {
    char [] expected = new char [3000];
    for (int i=0; i<expected.length; i++) {
      expected[i] = 'a';
    }
    assertThat(ResourceUtil.asString("ResourceUtil3000.bin"), is(new String(expected)));
  }
}
