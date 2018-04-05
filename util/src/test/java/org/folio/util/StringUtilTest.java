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
    "              ,           , ''",
    "              , UTF-8     , ''",
    "abc           ,           ,                      ",
    "abc           , UTF-8     , abc                  ",
    "key=a-umlaut-ä, ISO-8859-1, key%3Da-umlaut-%E4   ",
    "key=a-umlaut-ä, UTF-8     , key%3Da-umlaut-%C3%A4",
  })
  void urlencode(String source, String encoding, String expected) {
    assertThat(StringUtil.urlencode(source, encoding), is(expected));
  }

  @ParameterizedTest
  @CsvSource({
    "              , ''",
    "abc           , abc                  ",
    "key=a-umlaut-ä, key%3Da-umlaut-%C3%A4",
  })
  void urlencode(String source, String expected) {
    assertThat(StringUtil.urlencode(source), is(expected));
  }
}
