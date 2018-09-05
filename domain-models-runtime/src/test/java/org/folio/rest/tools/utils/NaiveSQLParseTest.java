package org.folio.rest.tools.utils;

import junitparams.JUnitParamsRunner;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import junitparams.Parameters;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class NaiveSQLParseTest {
  @Test
  @Parameters({
    "limit 'limit' ^limit 1",
    "^limit 'limit'",
    "limit 'limit' ^limit",
    "limit 'limit' ^limit 1",
    "limit 'limit limit limit' limit 'limit limit limit' ^limit",
    "LIMIT LIMIT ^LIMIT",
    "^LIMIT 1 SQL_SELECT_LIMIT 2",
    "^LIMIT 1 LIMIT_SQL_SELECT 2",
    "^limit 'limit''limit'",
    "^limit E'limit''limit'",
    "limit 'limit\\' ^limit",
    "^limit e'limit\\'limit'"
  })
  public void test1(String testCase) {
    int expectedPos = testCase.indexOf('^');

    String q = testCase.substring(0, expectedPos) + testCase.substring(expectedPos + 1);

    assertThat(NaiveSQLParse.getLastStartPos(q, "limit"), is(expectedPos));
    assertThat(NaiveSQLParse.getLastStartPos(q, "LIMIT"), is(expectedPos));
  }

  @Test
  @Parameters({
    "^order by 'limit'",
    "order byx 'order by' ^order by",
    "^ORDER BY 1 SQL_SELECT_ORDER BY 2",
    "^ORDER BY 1 ORDER BY_SQL_SELECT 2",
    "order by''^order by\"order by\""
  })
  public void test2(String testCase) {
    int expectedPos = testCase.indexOf('^');

    String q = testCase.substring(0, expectedPos) + testCase.substring(expectedPos + 1);
    assertThat(NaiveSQLParse.getLastStartPos(q, "order by"), is(expectedPos));
  }

}
