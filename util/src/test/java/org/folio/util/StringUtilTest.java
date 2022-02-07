package org.folio.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import org.folio.okapi.testing.UtilityClassTester;
import org.junit.jupiter.api.Assertions;
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
    "         , \"\"",  // null
    "''       , \"\"",  // empty string
    "a        , \"a\"",
    "foo      , \"foo\"",
    "foo* bar*, \"foo\\* bar\\*\"",
    "\\*?^    , \"\\\\\\*\\?\\^\"",
    "*?\\*?\\ , \"\\*\\?\\\\\\*\\?\\\\\"",
  })
  void cqlEncode(String s, String encoded) {
    assertThat(StringUtil.cqlEncode(s), is(encoded));

    StringBuilder stringBuilder = new StringBuilder("x*y");
    String actual = StringUtil.appendCqlEncoded(stringBuilder, s).toString();
    assertThat(actual, is("x*y" + encoded));
  }

  @Test
  void appendCqlEncodedIOException() throws IOException {
    Writer writer = Writer.nullWriter();
    writer.close();
    Assertions.assertThrows(UncheckedIOException.class, () -> StringUtil.appendCqlEncoded(writer, "foo"));
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
