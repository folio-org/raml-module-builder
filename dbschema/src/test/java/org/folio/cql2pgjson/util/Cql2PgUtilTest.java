package org.folio.cql2pgjson.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.stream.Stream;

import org.folio.rest.testing.UtilityClassTester;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class Cql2PgUtilTest {
  @Test
  void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(Cql2PgUtil.class);
  }

  @ParameterizedTest
  @CsvSource({
    "a      , tab.jsonb->'a'",
    "a.b    , tab.jsonb->'a'->'b'",
    "a.b.c  , tab.jsonb->'a'->'b'->'c'",
    "abc    , tab.jsonb->'abc'",
    "abc.xyz, tab.jsonb->'abc'->'xyz'",
  })
  void cqlNameAsSqlJson(String cqlName, String sql) {
    assertThat(Cql2PgUtil.cqlNameAsSqlJson("tab.jsonb", cqlName), is(sql));

    StringBuilder x = new StringBuilder("x ");
    Cql2PgUtil.appendCqlNameAsSqlJson("tab.jsonb", cqlName, x);
    assertThat(x.toString(), is("x " + sql));

    StringBuilder y = new StringBuilder("y ");
    Cql2PgUtil.appendCqlNameAsSqlJson("tab.jsonb", "u" + cqlName + "v", 1, 1 + cqlName.length(), y);
    assertThat(y.toString(), is("y " + sql));
  }

  @ParameterizedTest
  @CsvSource({
    "a      , tab.jsonb->>'a'",
    "a.b    , tab.jsonb->'a'->>'b'",
    "a.b.c  , tab.jsonb->'a'->'b'->>'c'",
    "abc    , tab.jsonb->>'abc'",
    "abc.xyz, tab.jsonb->'abc'->>'xyz'",
  })
  void cqlNameAsSqlText(String cqlName, String sql) {
    assertThat(Cql2PgUtil.cqlNameAsSqlText("tab.jsonb", cqlName), is(sql));

    StringBuilder x = new StringBuilder("x ");
    Cql2PgUtil.appendCqlNameAsSqlText("tab.jsonb", cqlName, x);
    assertThat(x.toString(), is("x " + sql));

    StringBuilder y = new StringBuilder("y ");
    Cql2PgUtil.appendCqlNameAsSqlText("tab.jsonb", "u" + cqlName + "v", 1, 1 + cqlName.length(), y);
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
    assertThat(Cql2PgUtil.quoted(s), is(quoted));

    StringBuilder x = new StringBuilder("x ");
    Cql2PgUtil.appendQuoted(s, x);
    assertThat(x.toString(), is("x " + quoted));

    StringBuilder y = new StringBuilder("y ");
    Cql2PgUtil.appendQuoted("u" + s + "v", 1, 1 + s.length(), y);
    assertThat(y.toString(), is("y " + quoted));
  }
}
