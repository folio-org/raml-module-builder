package org.z3950.zing.cql.cql2pgjson;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.folio.cql2pgjson.exception.CQL2PgJSONException;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

/**
 * Performance test of index usage.  This tests how Postgres uses indexes, it does
 * not test any other class or function.
 * <p>
 * Only runs if environment variable TEST_PERFORMANCE=yes, for example <br>
 * TEST_PERFORMANCE=yes mvn test
 */
@RunWith(JUnitParamsRunner.class)
public class IndexPerformanceTest extends DatabaseTestBase {
  private static String valueJsonbToFind = "'\"a1b2c3d4e5f6 xxxx\"'";
  private static String valueStringToFind = valueJsonbToFind.replace("\"", "");

  @BeforeClass
  public static void beforeClass() {
    Assume.assumeTrue("TEST_PERFORMANCE=yes", "yes".equals(System.getenv("TEST_PERFORMANCE")));
    setupDatabase();
    runSqlFile("indexPerformanceTest.sql");
  }

  @AfterClass
  public static void afterClass() {
    closeDatabase();
  }

  private void in50ms(String where) {
    AnalyseResult analyseResult = analyse("SELECT * FROM config_data " + where);
    float ms = analyseResult.getExecutionTimeInMs();
    System.out.println(where);
    System.out.println(analyseResult.getMessage());
    final float MAX = 50;
    if (ms > MAX) {
      fail("Expected at most " + MAX + " ms, but it runs " + ms + " ms: " + where);
    }
    if (! analyseResult.getMessage().contains(" idx_value ") &&
        ! analyseResult.getMessage().contains(" idx_num ")     ) {
      fail("Query plan neither uses idx_value nor idx_num: " + where + "\n" + analyseResult.getMessage());
    }
  }

  @Test
  @Parameters({
    "jsonb->'value'",
    "jsonb->>'value'",
    "(jsonb->'value')::text",
    "lower(jsonb->>'value')",
    "lower((jsonb->'value')::text)",
  })
  public void trueOrderByUsesIndex(String index) {
    runSqlStatement("DROP INDEX IF EXISTS idx_value;");
    runSqlStatement("CREATE INDEX idx_value ON config_data ((" + index + "))");
    in50ms("WHERE TRUE ORDER BY " + index + " ASC  LIMIT 30;");
    in50ms("WHERE TRUE ORDER BY " + index + " DESC LIMIT 30;");
    String match = valueStringToFind;
    if (index.contains("->'")) {
      match = valueJsonbToFind;
    }
    in50ms("WHERE " + index + " = " + match);
  }

  private void likeUsesIndex(String index, String sort) {
    String match = "'\"a1%\"'";
    if (index.contains("->>")) {
      match = match.replace("\"", "");
    }
    in50ms("WHERE lower(f_unaccent(" + index + ")) LIKE " + match
        + " ORDER BY lower(f_unaccent(" + index + ")) " + sort + ", " + index + sort + "LIMIT 30;");
  }

  @Test
  @Parameters({
    "jsonb->>'value'",
    "(jsonb->'value')::text",
  })
  public void likeUsesIndex(String index) {
    runSqlStatement("DROP INDEX IF EXISTS idx_value;");
    String finalIndex = "lower(f_unaccent(" + index + "))";
    runSqlStatement("CREATE INDEX idx_value ON config_data ((" + finalIndex + ") text_pattern_ops);");
    String [] sorts = { " ASC  ", " DESC " };
    for (String sort : sorts) {
      likeUsesIndex(index, sort);
    }
  }

  @Test
  public void cqlValue() throws CQL2PgJSONException, IOException {
    runSqlStatement("DROP INDEX IF EXISTS idx_value;");
    runSqlStatement("CREATE INDEX idx_value ON config_data "
        + "((lower(f_unaccent(jsonb->>'value'))) text_pattern_ops);");
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("jsonb");
    in50ms("WHERE " + cql2pgJson.cql2pgJson("value == a1* sortBy value"));
    in50ms("WHERE " + cql2pgJson.cql2pgJson("value == b2* sortBy value"));
    // https://issues.folio.org/browse/UICHKOUT-39 "Checkout is broken"
    in50ms("WHERE " + cql2pgJson.cql2pgJson("value == 036000291452 sortBy value"));
    in50ms("WHERE " + cql2pgJson.cql2pgJson("value ==  36000291452 sortBy value"));
  }

  @Test
  public void cqlNum() throws CQL2PgJSONException, IOException {
    runSqlStatement("DROP INDEX IF EXISTS idx_num;");
    runSqlStatement("CREATE INDEX idx_num ON config_data ((jsonb->'num'));");
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("jsonb");
    in50ms("WHERE " + cql2pgJson.cql2pgJson("num == 0.003  sortBy num"));
    in50ms("WHERE " + cql2pgJson.cql2pgJson("num == 0.0040 sortBy num"));
    in50ms("WHERE " + cql2pgJson.cql2pgJson("num == 0.005  sortBy num"));
    in50ms("WHERE " + cql2pgJson.cql2pgJson("num == 0.0060 sortBy num"));
  }
}

