package org.folio.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.folio.okapi.testing.UtilityClassTester;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class UuidUtilTest {
  @Test
  void utilityClass() {
    UtilityClassTester.assertUtilityClass(UuidUtil.class);
  }

  @Test
  void isUuidNull() {
    assertThat(UuidUtil.isUuid(null), is(false));
  }

  @Test
  void isLooseUuidNull() {
    assertThat(UuidUtil.isLooseUuid(null), is(false));
  }

  @ParameterizedTest
  @CsvSource({
    "''",   // empty String
    "' '",  // single space
    "a",
    "1111",
    "FFFFFFFF-FFFF-5FFF-BFFF-FFFFFFFFFFF",
    "FFFFFFFF-FFFF-5FFF-BFFF-FFFFFFFFFFFFF",
  })
  void isInvalidUuid(String s) {
    assertThat(UuidUtil.isLooseUuid(s), is(false));
    assertThat(UuidUtil.isUuid(s), is(false));
  }

  @ParameterizedTest
  @CsvSource({
    "12345678-1234-1234-890a-1234567890ab",
    "abcdABCD-abcd-1BCD-BCDE-abcdefABCDEF",
    "ffffffff-ffff-5fff-bfff-ffffffffffff",
    "FFFFFFFF-FFFF-5FFF-BFFF-FFFFFFFFFFFF",
  })
  void isValidUuid(String s) {
    assertThat(UuidUtil.isLooseUuid(s), is(true));
    assertThat(UuidUtil.isUuid(s), is(true));
  }

  @ParameterizedTest
  @CsvSource({
    "00000000-0000-0000-0000-000000000000",
    "11111111-1111-1111-1111-111111111111",
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "ffffffff-ffff-ffff-ffff-ffffffffffff",
  })
  void isLooseUuidButInvalidUuid(String s) {
    assertThat(UuidUtil.isLooseUuid(s), is(true));
    assertThat(UuidUtil.isUuid(s), is(false));
  }
}
