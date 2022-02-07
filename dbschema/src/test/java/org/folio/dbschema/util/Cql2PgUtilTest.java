package org.folio.dbschema.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.stream.Stream;

import org.folio.okapi.testing.UtilityClassTester;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class Cql2PgUtilTest {
  @Test
  void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(SqlUtil.Cql2PgUtil.class);
  }

  @ParameterizedTest
  @CsvSource({
    "a         , tab.jsonb->'a'",
    "a.b       , tab.jsonb->'a'->'b'",
    "a.b.c     , tab.jsonb->'a'->'b'->'c'",
    "abc       , tab.jsonb->'abc'",
    "abc.xyz   , tab.jsonb->'abc'->'xyz'",
    "a'bc.'xyz', tab.jsonb->'a''bc'->'''xyz'''",  // sql injection test
  })
  void cqlNameAsSqlJson(String cqlName, String sql) {
    assertThat(SqlUtil.Cql2PgUtil.cqlNameAsSqlJson("tab.jsonb", cqlName), is(sql));

    StringBuilder x = new StringBuilder("x ");
    SqlUtil.Cql2PgUtil.appendCqlNameAsSqlJson("tab.jsonb", cqlName, x);
    assertThat(x.toString(), is("x " + sql));

    StringBuilder y = new StringBuilder("y ");
    SqlUtil.Cql2PgUtil.appendCqlNameAsSqlJson("tab.jsonb", "u" + cqlName + "v", 1, 1 + cqlName.length(), y);
    assertThat(y.toString(), is("y " + sql));
  }

  @ParameterizedTest
  @CsvSource({
    "a         , tab.jsonb->>'a'",
    "a.b       , tab.jsonb->'a'->>'b'",
    "a.b.c     , tab.jsonb->'a'->'b'->>'c'",
    "abc       , tab.jsonb->>'abc'",
    "abc.xyz   , tab.jsonb->'abc'->>'xyz'",
    "a'bc.'xyz', tab.jsonb->'a''bc'->>'''xyz'''",  // sql injection test
  })
  void cqlNameAsSqlText(String cqlName, String sql) {
    assertThat(SqlUtil.Cql2PgUtil.cqlNameAsSqlText("tab.jsonb", cqlName), is(sql));

    StringBuilder x = new StringBuilder("x ");
    SqlUtil.Cql2PgUtil.appendCqlNameAsSqlText("tab.jsonb", cqlName, x);
    assertThat(x.toString(), is("x " + sql));

    StringBuilder y = new StringBuilder("y ");
    SqlUtil.Cql2PgUtil.appendCqlNameAsSqlText("tab.jsonb", "u" + cqlName + "v", 1, 1 + cqlName.length(), y);
    assertThat(y.toString(), is("y " + sql));
  }

  static Stream<Arguments> quoted() {
    return Stream.of(
        Arguments.of(""           , "''"),
        Arguments.of("'"          , "''''"),
        Arguments.of("''"         , "''''''"),
        Arguments.of("a"          , "'a'"),
        Arguments.of("'a'"        , "'''a'''"),
        Arguments.of("It's cool"  , "'It''s cool'"),
        Arguments.of("Rock'n'roll", "'Rock''n''roll'")
        );
  }

  @ParameterizedTest
  @MethodSource
  void quoted(String s, String quoted) {
    assertThat(SqlUtil.Cql2PgUtil.quoted(s), is(quoted));

    StringBuilder x = new StringBuilder("x ");
    SqlUtil.Cql2PgUtil.appendQuoted(s, x);
    assertThat(x.toString(), is("x " + quoted));

    StringBuilder y = new StringBuilder("y ");
    SqlUtil.Cql2PgUtil.appendQuoted("u" + s + "v", 1, 1 + s.length(), y);
    assertThat(y.toString(), is("y " + quoted));
  }

  @ParameterizedTest
  @CsvSource({
    "false, false, x",
    "true,  false, lower(x)",
    "false, true,  f_unaccent(x)",
    "true,  true,  lower(f_unaccent(x))",
  })
  void wrapInLowerUnaccent(boolean lower, boolean unaccent, String expected) {
    assertThat(SqlUtil.Cql2PgUtil.wrapInLowerUnaccent("x", lower, unaccent), is(expected));
  }
}
