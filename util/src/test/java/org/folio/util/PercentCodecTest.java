package org.folio.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.folio.rest.testing.UtilityClassTester;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class PercentCodecTest {
  @Test
  void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(PercentCodec.class);
  }

  @ParameterizedTest
  @CsvSource({
    "              , ''",  // null -> empty
    "''            , ''",  // empty -> empty
    "abc           , abc",
    "status == open, status%20%3D%3D%20open",
    "key=a-umlaut-ä, key%3Da-umlaut-%C3%A4",
    "ääääääääää    , %C3%A4%C3%A4%C3%A4%C3%A4%C3%A4%C3%A4%C3%A4%C3%A4%C3%A4%C3%A4",
    "a b c         , a%20b%20c",
  })
  void uriEncode(String s, String expected) {
    assertThat(PercentCodec.encode(s).toString(), is(expected));
    StringBuffer stringBuffer = new StringBuffer("foo = ");
    PercentCodec.encode(stringBuffer, s);
    assertThat(stringBuffer.toString(), is("foo = " + expected));
  }
}
