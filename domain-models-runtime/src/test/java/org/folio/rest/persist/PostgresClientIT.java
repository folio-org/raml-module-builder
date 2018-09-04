package org.folio.rest.persist;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.UpdateSection;
import org.folio.rest.tools.utils.LogUtil;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class PostgresClientIT {

  static private final String TENANT = "tenant";
  /** table name */
  static private final String FOO = "foo";
  /** table name */
  static private final String INVALID_JSON = "invalid_json";
  static private final String INVALID_JSON_UUID = "49999999-4999-4999-8999-899999999999";
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
    LogUtil.setLevelForRootLoggers(oldLevel);
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
  public Timeout rule = Timeout.seconds(15);

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

  private <T> void assertSuccess(TestContext context, AsyncResult<T> result) {
    if (result.failed()) {
      context.fail(result.cause());
    }
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
    c1.closeClient(context.asyncAssertSuccess());
    c2.closeClient(context.asyncAssertSuccess());
    context.assertEquals(c1, c2, "same instance");
  }

  @Test
  public void getInstanceTenant(TestContext context) {
    PostgresClient c1 = PostgresClient.getInstance(vertx, TENANT);
    PostgresClient c2 = PostgresClient.getInstance(vertx, TENANT);
    c1.closeClient(context.asyncAssertSuccess());
    c2.closeClient(context.asyncAssertSuccess());
    context.assertEquals(c1, c2, "same instance");
  }

  @Test
  public void getNewInstance(TestContext context) {
    Async async = context.async();
    PostgresClient c1 = PostgresClient.getInstance(vertx);
    c1.closeClient(a -> {
      assertSuccess(context, a);
      PostgresClient c2 = PostgresClient.getInstance(vertx);
      context.assertNotEquals(c1, c2, "different instance");
      c2.closeClient(context.asyncAssertSuccess());
      async.complete();
    });
  }

  @Test
  public void getNewInstanceTenant(TestContext context) {
    Async async = context.async();
    PostgresClient c1 = PostgresClient.getInstance(vertx, TENANT);
    c1.closeClient(a -> {
      assertSuccess(context, a);
      PostgresClient c2 = PostgresClient.getInstance(vertx, TENANT);
      context.assertNotEquals(c1, c2, "different instance");
      c2.closeClient(context.asyncAssertSuccess());
      async.complete();
    });
  }

  @Test(expected = IllegalArgumentException.class)
  public void instanceDefaultSchema(TestContext context) {
    PostgresClient.getInstance(vertx, PostgresClient.DEFAULT_SCHEMA);
  }

  private void execute(TestContext context, String sql) {
    Async async = context.async();
    PostgresClient.getInstance(vertx).runSQLFile(sql, false, reply -> {
      assertSuccess(context, reply);
      for (String result : reply.result()) {
        context.fail(result);
      }
      async.complete();
    });
    async.await();
  }

  private void executeIgnore(TestContext context, String sql) {
    Level localOldLevel = LogManager.getRootLogger().getLevel();
    LogUtil.setLevelForRootLoggers(Level.FATAL);

    Async async = context.async();
    PostgresClient.getInstance(vertx).runSQLFile(sql, false, reply -> {
      assertSuccess(context, reply);
      async.complete();
    });
    async.await();

    LogUtil.setLevelForRootLoggers(localOldLevel);
  }

  private PostgresClient createTable(TestContext context,
      String tenant, String table, String tableDefinition) {
    String schema = PostgresClient.convertToPsqlStandard(tenant);
    execute(context, "CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;");
    execute(context, "DROP SCHEMA IF EXISTS " + schema + " CASCADE;");
    executeIgnore(context, "CREATE ROLE " + schema + " PASSWORD '" + tenant + "' NOSUPERUSER NOCREATEDB INHERIT LOGIN;");
    execute(context, "CREATE SCHEMA " + schema + " AUTHORIZATION " + schema);
    execute(context, "GRANT ALL PRIVILEGES ON SCHEMA " + schema + " TO " + schema);
    execute(context, "CREATE TABLE " + schema + "." + table + " (" + tableDefinition + ");");
    execute(context, "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA " + schema + " TO " + schema);
    return PostgresClient.getInstance(vertx, tenant);
  }

  /** create table a (i INTEGER) */
  private PostgresClient createA(TestContext context, String tenant) {
    return createTable(context, tenant, "a", "i INTEGER");
  }

  private PostgresClient createFoo(TestContext context) {
    return createTable(context, TENANT, FOO,
        "_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), jsonb JSONB NOT NULL");
  }

  private PostgresClient createInvalidJson(TestContext context) {
    String schema = PostgresClient.convertToPsqlStandard(TENANT);
    PostgresClient postgresClient = createTable(context, TENANT, INVALID_JSON,
        "_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), jsonb VARCHAR(99) NOT NULL");
    execute(context, "INSERT INTO " + schema + "." + INVALID_JSON + " VALUES "
        +"('" + INVALID_JSON_UUID + "', '}');");
    return postgresClient;
  }

  private void fillA(TestContext context, PostgresClient client, String tenant, int i) {
    Async async = context.async();
    String schema = PostgresClient.convertToPsqlStandard(tenant);
    execute(context, "INSERT INTO "  + schema + ".a (i) VALUES (" + i + ") ON CONFLICT DO NOTHING;");
    client.select("SELECT i FROM " + schema + ".a", reply2 -> {
      assertSuccess(context, reply2);
      context.assertEquals(i, reply2.result().getResults().get(0).getInteger(0));
      async.complete();
    });
    async.await();
  }

  private void selectAFail(TestContext context, PostgresClient client, String tenant) {
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
    // don't log expected access violation errors
    LogUtil.setLevelForRootLoggers(Level.FATAL);
    String tenant = "tenantSeparation";
    String tenant2 = "tenantSeparation2";

    PostgresClient c1 = createA(context, tenant);
    PostgresClient c2 = createA(context, tenant2);
    fillA(context, c1, tenant, 5);
    fillA(context, c2, tenant2, 8);
    // c1 must be blocked from accessing schema TENANT2
    selectAFail(context, c1, tenant2);
    // c2 must be blocked from accessing schema TENANT
    selectAFail(context, c2, tenant);
    c1.closeClient(context.asyncAssertSuccess());
    c2.closeClient(context.asyncAssertSuccess());
  }

/*  @Test
  public void parallel(TestContext context) {
    *//** number of parallel queries *//*
    int n = 20;
    *//** sleep time in milliseconds *//*
    double sleep = 150;
    String selectSleep = "select pg_sleep(" + sleep/1000 + ")";
    *//** maximum duration in milliseconds for the completion of all parallel queries
     * NOTE: seems like current embedded postgres does not run in parallel, only one concur connection?
     * this works fine when on a regular postgres, for not added the x4 *//*
    long maxDuration = (long) (n * sleep) * 4;
     create n queries in parallel, each sleeping for some time.
     * If vert.x properly processes them in parallel it finishes
     * in less than half of the time needed for sequential processing.

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
      long duration = System.currentTimeMillis() - start;
      client.closeClient(whenDone -> {});
      context.assertTrue(handler.succeeded());
      context.assertTrue(duration < maxDuration,
          "duration must be less than " + maxDuration + " ms, it is " + duration + " ms");
      async.complete();
    });
  }
*/

  public static class StringPojo {
    public String key;
    public StringPojo() {
      // required by ObjectMapper.readValue for JSON to POJO conversion
    }
    public StringPojo(String key) {
      this.key = key;
    }
  }

  /** simple test case */
  StringPojo xPojo = new StringPojo("x");
  /** a single quote may be used for SQL injection */
  StringPojo singleQuotePojo = new StringPojo("'");

  private String randomUuid() {
    return UUID.randomUUID().toString();
  }

  @Test
  public void deleteX(TestContext context) {
    createFoo(context)
      .delete(FOO, xPojo, context.asyncAssertSuccess());
  }

  @Ignore("fails: unterminated quoted identifier")
  @Test
  public void deleteSingleQuote(TestContext context) {
    createFoo(context)
      .delete(FOO, singleQuotePojo, context.asyncAssertSuccess());
  }

  @Test
  public void updateX(TestContext context) {
    createFoo(context)
      .update(FOO, xPojo, randomUuid(), context.asyncAssertSuccess());
  }

  @Test
  public void updateSingleQuote(TestContext context) {
    createFoo(context)
      .update(FOO, singleQuotePojo, randomUuid(), context.asyncAssertSuccess());
  }

  @Test
  public void updateSectionX(TestContext context) {
    UpdateSection updateSection = new UpdateSection();
    updateSection.addField("key").setValue("x");
    createFoo(context)
      .update(FOO, updateSection, (Criterion) null, false, context.asyncAssertSuccess());
  }

  @Ignore("fails: unterminated quoted identifier")
  @Test
  public void updateSectionSingleQuote(TestContext context) {
    UpdateSection updateSection = new UpdateSection();
    updateSection.addField("key").setValue("'");
    createFoo(context)
      .update(FOO, updateSection, (Criterion) null, false, context.asyncAssertSuccess());
  }

  @Test
  public void upsertX(TestContext context) {
    createFoo(context)
      .upsert(FOO, randomUuid(), xPojo, context.asyncAssertSuccess());
  }

  @Test
  public void upsertSingleQuote(TestContext context) {
    createFoo(context)
      .upsert(FOO, randomUuid(), singleQuotePojo, context.asyncAssertSuccess());
  }

  @Test
  public void saveBatchX(TestContext context) {
    Async async = context.async();
    List<Object> list = Collections.singletonList(xPojo);
    PostgresClient postgresClient = createFoo(context);
    postgresClient.saveBatch(FOO, list, res -> {
      assertSuccess(context, res);
      String id = res.result().getResults().get(0).getString(0);
      postgresClient.getById(FOO, id, get -> {
        context.assertEquals("x", get.result().getString("key"));
        async.complete();
      });
    });
  }

  @Test
  public void saveBatchJson(TestContext context) {
    Async async = context.async();
    JsonArray array = new JsonArray()
        .add("{ \"x\" : \"a\" }")
        .add("{ \"y\" : \"'\" }");
    createFoo(context).saveBatch(FOO, array, res -> {
      assertSuccess(context, res);
      context.assertEquals(2, res.result().getRows().size());
      context.assertEquals("_id", res.result().getColumnNames().get(0));
      async.complete();
    });
  }

  @Test
  public void saveBatchEmpty(TestContext context) {
    Async async = context.async();
    List<Object> list = Collections.emptyList();
    createFoo(context).saveBatch(FOO, list, res -> {
      assertSuccess(context, res);
      context.assertEquals(0, res.result().getRows().size());
      context.assertEquals("_id", res.result().getColumnNames().get(0));
      async.complete();
    });
  }

  @Test
  public void saveBatchSingleQuote(TestContext context) {
    List<Object> list = Collections.singletonList(singleQuotePojo);
    createFoo(context).saveBatch(FOO, list, context.asyncAssertSuccess());
  }

  @Test
  public void getByIdAsString(TestContext context) {
    Async async = context.async();
    PostgresClient postgresClient = createFoo(context);
    postgresClient.save(FOO, xPojo, res -> {
      assertSuccess(context, res);
      String id = res.result();
      postgresClient.getByIdAsString(FOO, id, get -> {
        assertSuccess(context, get);
        context.assertTrue(get.result().contains("\"key\""));
        context.assertTrue(get.result().contains(":"));
        context.assertTrue(get.result().contains("\"x\""));
        async.complete();
      });
    });
  }

  @Test
  public void getByIdAsPojo(TestContext context) {
    Async async = context.async();
    PostgresClient postgresClient = createFoo(context);
    postgresClient.save(FOO, xPojo, res -> {
      assertSuccess(context, res);
      String id = res.result();
      postgresClient.getById(FOO, id, StringPojo.class, get -> {
        assertSuccess(context, get);
        context.assertEquals(xPojo.key, get.result().key);
        async.complete();
      });
    });
  }

  @Test
  public void getByIdConnectionFailure(TestContext context) throws Exception {
    PostgresClient postgresClient = new PostgresClient(Vertx.vertx(), "nonexistingTenant");
    postgresClient.getByIdAsString(
        FOO, randomUuid(), context.asyncAssertFailure());
  }

  @Test
  public void getByIdFailure(TestContext context) {
    createFoo(context).getByIdAsString(
        "nonexistingTable", randomUuid(), context.asyncAssertFailure());
  }

  @Test
  public void getByIdInvalidJson(TestContext context) {
    // let the JSON to POJO conversion fail
    createInvalidJson(context).getById(
        INVALID_JSON, INVALID_JSON_UUID, StringPojo.class, context.asyncAssertFailure());
  }

  @Test
  public void getByIdEmpty(TestContext context) {
    Async async = context.async();
    createFoo(context).getByIdAsString(FOO, randomUuid(), res -> {
      assertSuccess(context, res);
      context.assertNull(res.result());
      async.complete();
    });
  }

  private PostgresClient insertXAndSingleQuotePojo(TestContext context, JsonArray ids) {
    Async async = context.async();
    PostgresClient postgresClient = createFoo(context);
    postgresClient.save(FOO, ids.getString(0), xPojo, res1 -> {
      assertSuccess(context, res1);
      postgresClient.save(FOO, ids.getString(1), singleQuotePojo, res2 -> {
        assertSuccess(context, res2);
        async.complete();
      });
    });
    async.await();
    return postgresClient;
  }

  @Test
  public void getByIdsAsString(TestContext context) {
    Async async = context.async();
    String id1 = randomUuid();
    String id2 = randomUuid();
    JsonArray ids = new JsonArray().add(id1).add(id2);
    insertXAndSingleQuotePojo(context, ids).getByIdAsString(FOO, ids, get -> {
      assertSuccess(context, get);
      context.assertEquals(2, get.result().size());
      context.assertTrue(get.result().get(id1).contains("\"x\""));
      context.assertTrue(get.result().get(id2).contains("\"'\""));
      async.complete();
    });
  }

  @Test
  public void getByIdsPojo(TestContext context) {
    Async async = context.async();
    String id1 = randomUuid();
    String id2 = randomUuid();
    JsonArray ids = new JsonArray().add(id1).add(id2);
    insertXAndSingleQuotePojo(context, ids).getById(FOO, ids, StringPojo.class, get -> {
      assertSuccess(context, get);
      context.assertEquals(2, get.result().size());
      context.assertEquals("x", get.result().get(id1).key);
      context.assertEquals("'", get.result().get(id2).key);
      async.complete();
    });
  }

  @Test
  public void getByIds(TestContext context) {
    Async async = context.async();
    String id1 = randomUuid();
    String id2 = randomUuid();
    JsonArray ids = new JsonArray().add(id1).add(id2);
    insertXAndSingleQuotePojo(context, ids).getById(FOO, ids, get -> {
      assertSuccess(context, get);
      context.assertEquals(2, get.result().size());
      context.assertEquals("x", get.result().get(id1).getString("key"));
      context.assertEquals("'", get.result().get(id2).getString("key"));
      async.complete();
    });
  }

  /** one random UUID in a JsonArray */
  private JsonArray randomUuidArray() {
    return new JsonArray().add(randomUuid());
  }

  @Test
  public void getByIdsConnectionFailure(TestContext context) throws Exception {
    PostgresClient postgresClient = new PostgresClient(Vertx.vertx(), "nonexistingTenant");
    postgresClient.getByIdAsString(FOO, randomUuidArray(), context.asyncAssertFailure());
  }

  @Test
  public void getByIdsFailure(TestContext context) {
    createFoo(context).getByIdAsString(
        "nonexistingTable", randomUuidArray(), context.asyncAssertFailure());
  }

  @Test
  public void getByIdsInvalidJson(TestContext context) {
    // let the JSON to POJO conversion fail
    createInvalidJson(context).getById(
        INVALID_JSON, new JsonArray().add(INVALID_JSON_UUID), StringPojo.class,
        context.asyncAssertFailure());
  }

  @Test
  public void getByIdsNotFound(TestContext context) {
    Async async = context.async();
    createFoo(context).getByIdAsString(FOO, randomUuidArray(), res -> {
      assertSuccess(context, res);
      context.assertTrue(res.result().isEmpty());
      async.complete();
    });
  }

  @Test
  public void getByIdsEmpty(TestContext context) {
    Async async = context.async();
    createFoo(context).getByIdAsString(FOO, new JsonArray(), res -> {
      assertSuccess(context, res);
      context.assertTrue(res.result().isEmpty());
      async.complete();
    });
  }

  @Test
  public void getByIdsNull(TestContext context) {
    Async async = context.async();
    createFoo(context).getByIdAsString(FOO, (JsonArray) null, res -> {
      assertSuccess(context, res);
      context.assertTrue(res.result().isEmpty());
      async.complete();
    });
  }
}
