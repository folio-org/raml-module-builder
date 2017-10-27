package org.folio.rest.persist;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class PostgresClientIT {
  static private final String TENANT = "tenant";
  static private Vertx vertx;

  private ByteArrayOutputStream myStdErrBytes = new ByteArrayOutputStream();
  private PrintStream myStdErr = new PrintStream(myStdErrBytes);
  private PrintStream oldStdErr = null;
  private Level oldLevel;

  @Before
  public void oldLevel() {
    oldLevel = LogManager.getRootLogger().getLevel();
  }

  @After
  public void restoreOldLevel() {
    LogManager.getRootLogger().setLevel(oldLevel);
  }

  @Before
  public void enableMyStdErr() {
    if (oldStdErr == null) {
      oldStdErr = System.err;
    }
    System.setErr(myStdErr);
    myStdErrBytes.reset();
  }

  @After
  public void disableMyStdErr() {
    if (oldStdErr == null) {
      return;
    }
    System.setErr(oldStdErr);
    oldStdErr = null;
  }

  @Rule
  public Timeout rule = Timeout.seconds(6);

  @BeforeClass
  public static void setUpClass() throws Exception {
    vertx = VertxUtils.getVertxWithExceptionHandler();

    String embed = System.getProperty("embed_postgres", "").toLowerCase().trim();
    if ("true".equals(embed)) {
      PostgresClient.setIsEmbedded(true);
      PostgresClient.getInstance(vertx).startEmbeddedPostgres();
    }
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    PostgresClient.stopEmbeddedPostgres();
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void closeClient(TestContext context) {
    PostgresClient c = PostgresClient.getInstance(vertx);
    context.assertNotNull(c.getClient(), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(c.getClient(), "getClient()");
  }

  @Test
  public void closeClientTenant(TestContext context) {
    PostgresClient c = PostgresClient.getInstance(vertx, TENANT);
    context.assertNotNull(c.getClient(), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(c.getClient(), "getClient()");
  }

  @Test
  public void closeClientTwice(TestContext context) {
    PostgresClient c = PostgresClient.getInstance(vertx);
    context.assertNotNull(c.getClient(), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(c.getClient(), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(c.getClient(), "getClient()");
  }

  @Test
  public void closeClientTwiceTenant(TestContext context) {
    PostgresClient c = PostgresClient.getInstance(vertx, TENANT);
    context.assertNotNull(c.getClient(), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(c.getClient(), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(c.getClient(), "getClient()");
  }

  @Test
  public void closeClientGetInstance(TestContext context) {
    PostgresClient c = PostgresClient.getInstance(vertx, TENANT);
    context.assertNotNull(c.getClient(), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(c.getClient(), "getClient()");
    c = PostgresClient.getInstance(vertx, TENANT);
    context.assertNotNull(c.getClient(), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(c.getClient(), "getClient()");
  }

  @Test
  public void getInstance(TestContext context) {
    PostgresClient c1 = PostgresClient.getInstance(vertx);
    PostgresClient c2 = PostgresClient.getInstance(vertx);
    context.assertEquals(c1, c2, "same instance");
  }

  @Test
  public void getInstanceTenant(TestContext context) {
    PostgresClient c1 = PostgresClient.getInstance(vertx, TENANT);
    PostgresClient c2 = PostgresClient.getInstance(vertx, TENANT);
    context.assertEquals(c1, c2, "same instance");
  }

  @Test
  public void getNewInstance(TestContext context) {
    Async async = context.async();
    PostgresClient c1 = PostgresClient.getInstance(vertx);
    c1.closeClient(a -> {
      PostgresClient c2 = PostgresClient.getInstance(vertx);
      context.assertNotEquals(c1, c2, "different instance");
      async.complete();
    });
  }

  @Test
  public void getNewInstanceTenant(TestContext context) {
    Async async = context.async();
    PostgresClient c1 = PostgresClient.getInstance(vertx, TENANT);
    c1.closeClient(a -> {
      PostgresClient c2 = PostgresClient.getInstance(vertx, TENANT);
      context.assertNotEquals(c1, c2, "different instance");
      async.complete();
    });
  }

  @Test(expected = IllegalArgumentException.class)
  public void instanceDefaultSchema(TestContext context) {
    PostgresClient.getInstance(vertx, PostgresClient.DEFAULT_SCHEMA);
  }

  private void dropSchemaRole(TestContext context, String tenant) {
    Async async = context.async();
    String schema = PostgresClient.convertToPsqlStandard(tenant);
    String sql =
        "REASSIGN OWNED BY " + schema + " TO postgres;\n"
      + "DROP OWNED BY " + schema + " CASCADE;\n"
      + "DROP ROLE IF EXISTS " + schema + ";\n";
    PostgresClient.getInstance(vertx).runSQLFile(sql, true, reply -> {
      context.assertTrue(reply.succeeded());
      // ignore errors
      async.complete();
    });
    async.await();
  }

  private void execute(TestContext context, String sql) {
    Async async = context.async();
    PostgresClient.getInstance(vertx).runSQLFile(sql, false, reply -> {
      context.assertTrue(reply.succeeded());
      for (String result : reply.result()) {
        context.fail(result);
      }
      async.complete();
    });
    async.await();
  }

  private void executeIgnore(TestContext context, String sql) {
    Async async = context.async();
    PostgresClient.getInstance(vertx).runSQLFile(sql, false, reply -> {
      context.assertTrue(reply.succeeded());
      async.complete();
    });
    async.await();
  }

  private void createSchema(TestContext context, String tenant) {
    String schema = PostgresClient.convertToPsqlStandard(tenant);
    executeIgnore(context, "CREATE ROLE " + schema + " PASSWORD '" + tenant + "' NOSUPERUSER NOCREATEDB INHERIT LOGIN;");
    execute(context, "GRANT " + schema + " TO " + schema);  // Grant membership in the role to that user
    execute(context, "CREATE SCHEMA IF NOT EXISTS " + schema + " AUTHORIZATION " + schema);
    execute(context, "GRANT ALL PRIVILEGES ON SCHEMA " + schema + " TO " + schema);
    execute(context, "CREATE TABLE IF NOT EXISTS " + schema + ".a ( i integer );");
    execute(context, "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA " + schema + " TO " + schema);
  }

  private void fillTable(TestContext context, PostgresClient client, String tenant, int i) {
    Async async = context.async();
    String schema = PostgresClient.convertToPsqlStandard(tenant);
    execute(context, "INSERT INTO "  + schema + ".a (i) VALUES (" + i + ") ON CONFLICT DO NOTHING;");
    client.select("SELECT i FROM " + schema + ".a", reply2 -> {
      if (! reply2.succeeded()) {
          context.fail(reply2.cause());
      };
      context.assertEquals(i, reply2.result().getResults().get(0).getInteger(0));
      async.complete();
    });
    async.await();
  }

  private void selectFail(TestContext context, PostgresClient client, String tenant) {
    Async async = context.async();
    String schema = PostgresClient.convertToPsqlStandard(tenant);
    client.select("SELECT i FROM " + schema + ".a", reply -> {
      context.assertFalse(reply.succeeded());
      async.complete();
    });
    async.await();
  }

  @Test
  public void tenantSeparation(TestContext context) {
    // suppress access violation errors
    LogManager.getRootLogger().setLevel(Level.FATAL);

    String tenant = "tenantSeparation";
    String tenant2 = "tenantSeparation2";

    dropSchemaRole(context, tenant);
    dropSchemaRole(context, tenant2);

    createSchema(context, tenant);
    createSchema(context, tenant2);
    PostgresClient c1 = PostgresClient.getInstance(vertx, tenant);
    PostgresClient c2 = PostgresClient.getInstance(vertx, tenant2);
    fillTable(context, c1, tenant, 5);
    fillTable(context, c2, tenant2, 8);
    // c1 must be blocked from accessing schema TENANT2
    selectFail(context, c1, tenant2);
    // c2 must be blocked from accessing schema TENANT
    selectFail(context, c2, tenant);
  }

  @Test
  public void parallel(TestContext context) {
    int n = 10;
    /** sleep time in milliseconds */
    double sleep = 100;
    String selectSleep = "select pg_sleep(" + sleep/1000 + ")";
    /** maximum duration in milliseconds for the completion of all parallel queries */
    long maxDuration = (long) (n * sleep / 2);
    /* create n queries in parallel, each sleeping for some time.
     * If vert.x properly processes them in parallel it finishes
     * in less than half of the time needed for sequential processing.
     */
    Async async = context.async();
    PostgresClient client = PostgresClient.getInstance(vertx);
    List<Future> futures = new ArrayList<>(n);
    for (int i=0; i<n; i++) {
      Future<ResultSet> future = Future.future();
      client.select(selectSleep, future.completer());
      futures.add(future);
    }
    long start = System.currentTimeMillis();
    CompositeFuture.all(futures).setHandler(handler -> {
      context.assertTrue(handler.succeeded());
      long duration = System.currentTimeMillis() - start;
      context.assertTrue(duration < maxDuration,
          "duration must be less than " + maxDuration + " ms, it is " + duration + " ms");
      async.complete();
    });
  }
}
