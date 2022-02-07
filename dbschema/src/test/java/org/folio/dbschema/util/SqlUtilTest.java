package org.folio.dbschema.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.folio.okapi.testing.UtilityClassTester;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SqlUtilTest {
  @Test
  void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(SqlUtil.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "a",
    "A",
    "_",
    "fooBar",
    "foo_bar",
    "a234567890123456789012345678901234567890123456789",
  })
  void validSqlIdentifier(String identifier) {
    SqlUtil.validateSqlIdentifier(identifier);
    assertThat("no exception", true);
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "",
    " ",
    "ä",
    "Ä",
    "0",
    "'",
    "\"",
    "&",
    "bäh",
    "foo'bar",
    "foo\"bar",
    "foo&bar",
    "a2345678901234567890123456789012345678901234567890",
  })
  void invalidSqlIdentifier(String identifier) {
    assertThrows(IllegalArgumentException.class, () -> SqlUtil.validateSqlIdentifier(identifier));
  }

  @Test
  void nullSqlIdentifier() {
    assertThrows(NullPointerException.class, () -> SqlUtil.validateSqlIdentifier(null));
  }
}
