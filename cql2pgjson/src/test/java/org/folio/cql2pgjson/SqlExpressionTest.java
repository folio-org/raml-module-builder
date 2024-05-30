package org.folio.cql2pgjson;

import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.hamcrest.CoreMatchers.is;


class SqlExpressionTest {

  @ParameterizedTest
  @CsvSource(textBlock = """
                  f == x, WHERE (jsonb->>'f') LIKE 'x'
                  g == x, WHERE (jsonb->>'g') LIKE 'x'
                  h = x, WHERE get_tsvector((jsonb->>'h')) @@ tsquery_phrase('x')
                  """)
  void identity(String cql, String expectedSql) throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("t");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/sqlExpression.json");
    assertThat(cql2pgJson.toSql(cql).toString(), is(expectedSql));
  }
}
