package org.folio.rest.persist;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.folio.rest.tools.utils.VertxUtils;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Foreign key performance test.
 * <p>
 * Only runs if environment variable TEST_PERFORMANCE=yes, for example <br>
 * TEST_PERFORMANCE=yes mvn verify
 */
@RunWith(VertxUnitRunner.class)
public class ForeignKeyPerformanceIT {
  static private Vertx vertx;

  @BeforeClass
  static public void setupClass(TestContext context) {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    Assume.assumeTrue("TEST_PERFORMANCE=yes", "yes".equals(System.getenv("TEST_PERFORMANCE")));
  }

  @AfterClass
  static public void teardownClass(TestContext context) {
    dropTables(context);
    vertx.close(context.asyncAssertSuccess());
  }

  static private void runSQL(TestContext context, String inputSql) {
    Async async = context.async();
    PostgresClient client = PostgresClient.getInstance(vertx);
    String sql = inputSql;
    if (! sql.endsWith(";\n")) {
      sql += ";\n";
    }
    client.runSqlFile(sql).onComplete(context.asyncAssertSuccess(x -> async.complete()));
    async.awaitSuccess(5000);
  }

  static private void dropTables(TestContext context) {
    String sql =
        "DROP TABLE IF EXISTS foreignkeyperformanceit_withouttrigger;\n"
      + "DROP TABLE IF EXISTS foreignkeyperformanceit_withtrigger;\n"
      + "DROP TABLE IF EXISTS foreignkeyperformanceit_uuid;\n";
    runSQL(context, sql);
  }

  static private String resourceAsString(String name) throws IOException {
    // to be replaced by ResourceUtil.asString(name) from submodule util

    // Implementation idea:
    // https://stackoverflow.com/questions/309424/read-convert-an-inputstream-to-a-string#35446009
    // "8. Using ByteArrayOutputStream and inputStream.read (JDK)"

    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(name)) {
      if (inputStream == null) {
        throw new FileNotFoundException("Resource not found: " + name);
      }
      ByteArrayOutputStream result = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024];
      int length;
      while ((length = inputStream.read(buffer)) != -1) {
        result.write(buffer, 0, length);
      }
      return result.toString(StandardCharsets.UTF_8.name());
    }
  }

  static private void createTables(TestContext context) throws IOException {
    Async async = context.async();
    String sql = resourceAsString("ForeignKeyPerformanceIT.sql");
    PostgresClient.getInstance(vertx).runSqlFile(sql)
    .onComplete(context.asyncAssertSuccess(x -> async.complete()));
    async.awaitSuccess(5000);
  }

  private long i = 0;

  /**
   * Return the number of inserts in 5 seconds.
   */
  private long inserts(TestContext context, String outputtable) {
    long iStart = i;
    long timeStart = System.currentTimeMillis();
    do {
      i++;
      String sql = String.format("INSERT INTO %s (i, jsonb) "
          + "SELECT %d, jsonb_build_object('i', %d, 'id', '00000000-0000-0000-0000-00000000000%d')",
          outputtable, i, i, i%4);
      runSQL(context, sql);
    } while (System.currentTimeMillis() < timeStart + 5000);
    long n = i - iStart;
    System.out.println(n + " transactions in 5 seconds " + outputtable);
    return n;
  }

  @Test
  public void performance(TestContext context) throws IOException {
    dropTables(context);
    createTables(context);

    final int RUNS = 5;
    long [] notrigger = new long [RUNS];
    long [] trigger = new long [RUNS];

    for (int run=0; run<RUNS; run++) {
      notrigger[run] = inserts(context, "foreignkeyperformanceit_withouttrigger");
      trigger[run]   = inserts(context, "foreignkeyperformanceit_withtrigger");
    }

    System.out.println("Avg: " + mean(notrigger) + " Inserts with no Trigger in 5 seconds");
    System.out.println("Avg: " + mean(trigger) + " Inserts with Trigger in 5 seconds");

    Arrays.sort(notrigger);
    Arrays.sort(trigger);
    System.out.println("Median: " + notrigger[2] + " transactions in 5 seconds without trigger");
    System.out.println("Median: " + trigger[2]   + " transactions in 5 seconds with trigger");

    context.assertTrue(trigger[2]*2 >= notrigger[2], "trigger has at least half the transactions of notrigger");
  }

  public static double mean(long[] m) {
    double sum = 0;
    for (int i = 0; i < m.length; i++) {
        sum += m[i];
    }
    return sum / m.length;
  }
}
