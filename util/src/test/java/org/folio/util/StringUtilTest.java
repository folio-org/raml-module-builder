package org.folio.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.folio.rest.testing.UtilityClassTester;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class StringUtilTest {
  @Test
  void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(StringUtil.class);
  }

  @ParameterizedTest
  @CsvSource({
    "abc           , UTF-8     , abc                  ",
    "key=a-umlaut-ä, ISO-8859-1, key%3Da-umlaut-%E4   ",
    "key=a-umlaut-ä, UTF-8     , key%3Da-umlaut-%C3%A4",
  })
  void urlEncodeDecode(String source, String encoding, String encoded) {
    assertThat(StringUtil.urlEncode(source, encoding), is(encoded));
    assertThat(StringUtil.urlDecode(encoded, encoding), is(source));
  }

  @ParameterizedTest
  @CsvSource({
    "abc           , abc                  ",
    "key=a-umlaut-ä, key%3Da-umlaut-%C3%A4",
  })
  void urlEncodeDecode(String source, String encoded) {
    assertThat(StringUtil.urlEncode(source), is(encoded));
    assertThat(StringUtil.urlDecode(encoded), is(source));
  }

  @ParameterizedTest
  @CsvSource({
    " ,      , ''",
    " , UTF-8, ''",
    "x,      ,   ",
  })
  void urlEncodeDecodeNull(String s, String encoding, String expected) {
    assertThat(StringUtil.urlEncode(s, encoding), is(expected));
    assertThat(StringUtil.urlDecode(s, encoding), is(expected));
  }

  @Test
  void urlEncodeDecodeNull() {
    assertThat(StringUtil.urlEncode(null), is(""));
    assertThat(StringUtil.urlDecode(null), is(""));
  }
}
