package org.folio.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.UncheckedIOException;

import org.folio.okapi.testing.UtilityClassTester;
import org.junit.jupiter.api.Test;

class ResourceUtilTest {
  @Test
  void utilityClass() {
    UtilityClassTester.assertUtilityClass(ResourceUtil.class);
  }

  @Test
  void nullFilename() {
    assertThrows(NullPointerException.class, () -> ResourceUtil.asString(null));
  }

  @Test
  void fileDoesNotExist() {
    assertThrows(UncheckedIOException.class, () -> ResourceUtil.asString("/foobar"));
  }

  @Test
  void readFromDir() {
    assertThat(ResourceUtil.asString("dir/resourceUtil.txt" , getClass()), is("some text"));
    assertThat(ResourceUtil.asString("/dir/resourceUtil.txt", getClass()), is("some text"));
  }

  @Test
  void readEmptyFile() {
    assertThat(ResourceUtil.asString("ResourceUtilEmpty.bin",  (Class<?>) null), is(""));
    assertThat(ResourceUtil.asString("/ResourceUtilEmpty.bin", (Class<?>) null), is(""));
  }

  @Test
  void readExampleFile() {
    assertThat(ResourceUtil.asString("ResourceUtilExample.bin"),  is("first line\numlauts: äöü\n"));
    assertThat(ResourceUtil.asString("/ResourceUtilExample.bin"), is("first line\numlauts: äöü\n"));
  }

  @Test
  void read3000() {
    char [] expected = new char [3000];
    for (int i=0; i<expected.length; i++) {
      expected[i] = 'a';
    }
    assertThat(ResourceUtil.asString("ResourceUtil3000.bin"),  is(new String(expected)));
    assertThat(ResourceUtil.asString("/ResourceUtil3000.bin"), is(new String(expected)));
  }
}
