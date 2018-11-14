package org.folio.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.folio.rest.testing.UtilityClassTester;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;

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
    assertThrows(FileNotFoundException.class, () -> ResourceUtil.asString("/foobar"));
  }

  @Test
  void readEmptyFile() throws IOException {
    assertThat(ResourceUtil.asString("ResourceUtilEmpty.bin"),  is(""));
    assertThat(ResourceUtil.asString("/ResourceUtilEmpty.bin"), is(""));
  }

  @Test
  void readFileFromOtherClass() {
    assertThrows(FileNotFoundException.class,
        () -> ResourceUtil.asString("ResourceUtilEmpty.bin",  Vertx.class));
    assertThrows(FileNotFoundException.class,
        () -> ResourceUtil.asString("/ResourceUtilEmpty.bin", Vertx.class));
  }

  @Test
  void readExampleFile() throws IOException {
    assertThat(ResourceUtil.asString("ResourceUtilExample.bin"),  is("first line\numlauts: äöü\n"));
    assertThat(ResourceUtil.asString("/ResourceUtilExample.bin"), is("first line\numlauts: äöü\n"));
  }

  @Test
  void read3000() throws IOException {
    char [] expected = new char [3000];
    for (int i=0; i<expected.length; i++) {
      expected[i] = 'a';
    }
    assertThat(ResourceUtil.asString("ResourceUtil3000.bin"),  is(new String(expected)));
    assertThat(ResourceUtil.asString("/ResourceUtil3000.bin"), is(new String(expected)));
  }
}
