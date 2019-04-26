package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.UpdateSection;
import org.folio.rest.persist.facets.FacetField;
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
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.asyncsql.impl.PostgreSQLConnectionImpl;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLRowStream;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.jaxrs.model.Facet;
import org.folio.rest.jaxrs.model.ResultInfo;

@RunWith(VertxUnitRunner.class)
public class PostgresClientIT {
  private static final Logger log = LoggerFactory.getLogger(PostgresClientIT.class);

  static private final String TENANT = "tenant";
  /** table name */
  static private final String FOO = "foo";
  /** table name of something that does not exist */
  static private final String BAR = "bar";
  /** table name */
  static private final String INVALID_JSON = "invalid_json";
  static private final String INVALID_JSON_UUID = "49999999-4999-4999-8999-899999999999";
  static private final String MOCK_POLINES_TABLE = "mock_po_lines";
  static private Vertx vertx = null;

  /** Log4j2 logging level */
  private Level oldRootLevel;
  private PostgresClient postgresClient;

  @Rule
  public Timeout rule = Timeout.seconds(15);

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4j2LogDelegateFactory");
  }

  @BeforeClass
  public static void doesNotCompleteOnWindows() {
    final String os = System.getProperty("os.name").toLowerCase();
    org.junit.Assume.assumeFalse(os.contains("win")); // RMB-261
  }

  @BeforeClass
  public static void setUpClass(TestContext context) throws Exception {
    vertx = VertxUtils.getVertxWithExceptionHandler();

    String embed = System.getProperty("embed_postgres", "").toLowerCase().trim();
    if ("true".equals(embed)) {
      PostgresClient.setIsEmbedded(true);
      PostgresClient.getInstance(vertx).startEmbeddedPostgres();
    }

    // fail the complete test class if the connection to postgres doesn't work
    PostgresClient.getInstance(vertx).execute("SELECT 1", context.asyncAssertSuccess());
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    PostgresClient.stopEmbeddedPostgres();
    if (vertx != null) {
      vertx.close(context.asyncAssertSuccess());
      vertx = null;
    }
  }

  private Level getRootLevel() {
    LoggerContext loggerContext = LoggerContext.getContext(false);
    LoggerConfig loggerConfig = loggerContext.getConfiguration().getRootLogger();
    return loggerConfig.getLevel();
  }

  private void setRootLevel(Level newRootLevel) {
    Configurator.setRootLevel(newRootLevel);
  }

  @Before
  public void setUp() {
    oldRootLevel = getRootLevel();
    setRootLevel(Level.ERROR);

    postgresClient = null;
  }

  @After
  public void tearDown(TestContext context) {
    if (postgresClient != null) {
      postgresClient.closeClient(context.asyncAssertSuccess());
      postgresClient = null;
    }

    setRootLevel(oldRootLevel);
  }

  private <T> void assertSuccess(TestContext context, AsyncResult<T> result) {
    if (result.failed()) {
      setRootLevel(Level.DEBUG);
      context.fail(result.cause());
    }
  }

  /**
   * Similar to context.asyncAssertSuccess(resultHandler) but the type of the resultHandler
   * is Handler<AsyncResult<SQLConnection>> and not Handler<SQLConnection>.
   * Usage: postgresClient.startTx(asyncAssertTx(context, trans ->
   */
  private Handler<AsyncResult<SQLConnection>> asyncAssertTx(
      TestContext context, Handler<AsyncResult<SQLConnection>> resultHandler) {

    Async async = context.async();
    return trans -> {
      if (trans.failed()) {
        setRootLevel(Level.DEBUG);
        context.fail(trans.cause());
      }
      resultHandler.handle(trans);
      async.complete();
    };
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
  public void closeAllClients(TestContext context) {
    PostgresClient c = PostgresClient.getInstance(vertx);
    context.assertNotNull(c.getClient(), "getClient()");
    PostgresClient.closeAllClients();
  }

  @Test
  public void getInstance(TestContext context) {
    PostgresClient c1 = PostgresClient.getInstance(vertx);
    PostgresClient c2 = PostgresClient.getInstance(vertx);
    context.assertEquals("public", c1.getTenantId());
    context.assertEquals("public", c2.getTenantId());
    context.assertEquals("raml_module_builder", PostgresClient.getModuleName());
    context.assertEquals("public_raml_module_builder", c1.getSchemaName());
    context.assertEquals("public_raml_module_builder", c2.getSchemaName());
    c1.closeClient(context.asyncAssertSuccess());
    c2.closeClient(context.asyncAssertSuccess());
    context.assertEquals(c1, c2, "same instance");
  }

  @Test
  public void getInstanceTenant(TestContext context) {
    PostgresClient c1 = PostgresClient.getInstance(vertx, TENANT);
    PostgresClient c2 = PostgresClient.getInstance(vertx, TENANT);
    context.assertEquals(TENANT, c1.getTenantId());
    context.assertEquals(TENANT, c2.getTenantId());
    context.assertEquals("raml_module_builder", PostgresClient.getModuleName());
    context.assertEquals(TENANT + "_raml_module_builder", c1.getSchemaName());
    context.assertEquals(TENANT + "_raml_module_builder", c2.getSchemaName());
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
    PostgresClient c = PostgresClient.getInstance(vertx);
    c.getClient().update(sql, reply -> {
      c.closeClient(close -> {
        if (reply.failed() || close.failed()) {
          setRootLevel(Level.DEBUG);
        }
        assertSuccess(context, reply);
        assertSuccess(context, close);
        async.complete();
      });
    });
    async.awaitSuccess(5000);
  }

  private void executeIgnore(TestContext context, String sql) {
    Async async = context.async();
    Level oldLevel = getRootLevel();
    setRootLevel(Level.FATAL);
    PostgresClient c = PostgresClient.getInstance(vertx);
    c.getClient().update(sql, reply -> {
      c.closeClient(close -> {
        setRootLevel(oldLevel);
        assertSuccess(context, close);
        async.complete();
      });
    });
    async.awaitSuccess(5000);
  }

  private PostgresClient postgresClient(String tenant) {
    try {
      postgresClient = PostgresClient.getInstance(vertx, tenant);
    } catch (Throwable e) {
      setRootLevel(Level.DEBUG);
      throw e;
    }
    return postgresClient;
  }

  private PostgresClient postgresClient() {
    return postgresClient(TENANT);
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
    return postgresClient(tenant);
  }

  /** create table a (i INTEGER) */
  private PostgresClient createA(TestContext context, String tenant) {
    return createTable(context, tenant, "a", "i INTEGER");
  }

  private PostgresClient createFoo(TestContext context) {
    return createTable(context, TENANT, FOO,
        "_id UUID PRIMARY KEY , jsonb JSONB NOT NULL");
  }

  /** bar's primary key is "id" without underscore */
  private PostgresClient createBarIdHasNoUnderscore(TestContext context) {
    postgresClient = createTable(context, "bartenant", "bar",
        "id UUID PRIMARY KEY, jsonb JSONB NOT NULL");
    postgresClient.setIdField("id");
    return postgresClient;
  }

  private PostgresClient createInvalidJson(TestContext context) {
    String schema = PostgresClient.convertToPsqlStandard(TENANT);
    postgresClient = createTable(context, TENANT, INVALID_JSON,
        "_id UUID PRIMARY KEY, jsonb VARCHAR(99) NOT NULL");
    execute(context, "INSERT INTO " + schema + "." + INVALID_JSON + " VALUES "
        +"('" + INVALID_JSON_UUID + "', '}');");
    return postgresClient;
  }

  private void fillA(TestContext context, PostgresClient client, String tenant, int i) {
    Async async = context.async();
    String schema = PostgresClient.convertToPsqlStandard(tenant);
    execute(context, "INSERT INTO "  + schema + ".a (i) VALUES (" + i + ") ON CONFLICT DO NOTHING;");
    client.select("SELECT i FROM " + schema + ".a", context.asyncAssertSuccess(get -> {
      context.assertEquals(i, get.getResults().get(0).getInteger(0));
      async.complete();
    }));
    async.awaitSuccess(5000);
  }

  private void selectAFail(TestContext context, PostgresClient client, String tenant) {
    Async async = context.async();
    String schema = PostgresClient.convertToPsqlStandard(tenant);
    client.select("SELECT i FROM " + schema + ".a", context.asyncAssertFailure(reply -> {
      async.complete();
    }));
    async.awaitSuccess(5000);
  }

  @Test
  public void tenantSeparation(TestContext context) {
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
  public void updateNullConnection1(TestContext context) {
    postgresClientNullConnection()
      .update(FOO, xPojo, randomUuid(), context.asyncAssertFailure());
  }

  @Test
  public void updateGetConnectionFails1(TestContext context) {
    postgresClientGetConnectionFails()
      .update(FOO, xPojo, randomUuid(), context.asyncAssertFailure());
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

  @Test
  public void updateNullConnection2(TestContext context) {
    UpdateSection updateSection = new UpdateSection();
    updateSection.addField("key").setValue("x");
    postgresClientNullConnection().
      update(FOO, updateSection, (Criterion) null, false, context.asyncAssertFailure());
  }

  @Test
  public void updateGetConnectionFails2(TestContext context) {
    UpdateSection updateSection = new UpdateSection();
    updateSection.addField("key").setValue("x");
    postgresClientGetConnectionFails().
      update(FOO, updateSection, (Criterion) null, false, context.asyncAssertFailure());
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
    List<Object> list = Collections.singletonList(xPojo);
    postgresClient = createFoo(context);
    postgresClient.saveBatch(FOO, list, context.asyncAssertSuccess(save -> {
      String id = save.getResults().get(0).getString(0);
      postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
        context.assertEquals("x", get.getString("key"));
      }));
    }));
  }

  @Test
  public void saveBatchXTrans(TestContext context) {
    List<Object> list = Collections.singletonList(xPojo);
    postgresClient = createFoo(context);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.saveBatch(trans, FOO, list, context.asyncAssertSuccess(save -> {
        final String id = save.getResults().get(0).getString(0);
        postgresClient.endTx(trans, context.asyncAssertSuccess(end -> {
          postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
            context.assertEquals("x", get.getString("key"));
          }));
        }));
      }));
    }));
  }

  @Test
  public void saveBatchXTrans2(TestContext context) {
    log.fatal("started saveBatchXTrans2");
    List<Object> list = new LinkedList<>();
    list.add(context);
    postgresClient = createFoo(context);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.saveBatch(trans, FOO, list, context.asyncAssertFailure(save -> {
        // postgresClient.endTx(trans, context.asyncAssertSuccess());
      }));
    }));
  }

  @Test
  public void saveBatchNullConnection(TestContext context) {
    log.fatal("saveBatchNullConnection started");
    List<Object> list = Collections.singletonList(xPojo);
    postgresClientNullConnection().saveBatch(FOO, list, context.asyncAssertFailure());
  }

  @Test
  public void saveBatchGetConnectionFails(TestContext context) {
    List<Object> list = Collections.singletonList(xPojo);
    postgresClientGetConnectionFails().saveBatch(FOO, list, context.asyncAssertFailure());
  }

  @Test
  public void saveBatchJson(TestContext context) {
    JsonArray array = new JsonArray()
        .add("{ \"x\" : \"a\" }")
        .add("{ \"y\" : \"'\" }");
    createFoo(context).saveBatch(FOO, array, context.asyncAssertSuccess(res -> {
      context.assertEquals(2, res.getRows().size());
      context.assertEquals("_id", res.getColumnNames().get(0));
    }));
  }

  @Test
  public void saveBatchJsonFail(TestContext context) {
    JsonArray array = new JsonArray()
        .add("{ \"x\" : \"a\" }")
        .add("{ \"y\" : \"'\" }");
    createFoo(context).saveBatch(BAR, array, context.asyncAssertFailure());
  }

  @Test
  public void saveTrans(TestContext context) {
    postgresClient = createFoo(context);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.save(trans, FOO, xPojo, context.asyncAssertSuccess(id -> {
        Criterion filter = new Criterion(new Criteria().addField("_id").setJSONB(false)
            .setOperation("=").setValue("'" + id  + "'"));
        postgresClient.get(trans, FOO, StringPojo.class, filter, false, false, context.asyncAssertSuccess(reply1 -> {
          context.assertEquals(1, reply1.getResults().size());
          context.assertEquals("x", reply1.getResults().get(0).key);
          postgresClient.rollbackTx(trans, context.asyncAssertSuccess(rollback -> {
            postgresClient.get(FOO, StringPojo.class, filter, false, false, context.asyncAssertSuccess(reply2 -> {
              context.assertEquals(0, reply2.getResults().size());
            }));
          }));
        }));
      }));
    }));
  }

  @Test
  public void saveTransId(TestContext context) {
    String id = randomUuid();
    postgresClient = createFoo(context);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.save(trans, FOO, id, xPojo, context.asyncAssertSuccess(res -> {
        context.assertEquals(id, res);
        Criterion filter = new Criterion(new Criteria().addField("_id").setJSONB(false)
            .setOperation("=").setValue("'" + id  + "'"));
        postgresClient.get(trans, FOO, StringPojo.class, filter, false, false, context.asyncAssertSuccess(reply -> {
          context.assertEquals(1, reply.getResults().size());
          context.assertEquals("x", reply.getResults().get(0).key);
          postgresClient.rollbackTx(trans, context.asyncAssertSuccess(rollback -> {
            postgresClient.get(FOO, StringPojo.class, filter, false, false, context.asyncAssertSuccess(reply2 -> {
              context.assertEquals(0, reply2.getResults().size());
            }));
          }));
        }));
      }));
    }));
  }

  @Test
  public void saveTransSyntaxError(TestContext context) {
    postgresClient = createFoo(context);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.save(trans, "'", xPojo, context.asyncAssertFailure(save -> {
        postgresClient.rollbackTx(trans, context.asyncAssertSuccess());
      }));
    }));
  }

  @Test
  public void saveTransIdSyntaxError(TestContext context) {
    String id = randomUuid();
    postgresClient = createFoo(context);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.save(trans, "'", id, xPojo, context.asyncAssertFailure(save -> {
        postgresClient.rollbackTx(trans, context.asyncAssertSuccess());
      }));
    }));
  }

  @Test
  public void saveTransNull(TestContext context) {
    postgresClient = createFoo(context);
    AsyncResult<SQLConnection> trans = null;
    setRootLevel(Level.FATAL);
    postgresClient.save(trans, FOO, xPojo, context.asyncAssertFailure());
  }

  @Test
  public void saveConnectionNullConnection(TestContext context) {
    String uuid = randomUuid();
    postgresClientNullConnection().save(FOO, uuid, xPojo, context.asyncAssertFailure());
  }

  @Test
  public void saveConnectionGetConnectionFails(TestContext context) {
    String uuid = randomUuid();
    postgresClientGetConnectionFails().save(FOO, uuid, xPojo, context.asyncAssertFailure());
  }

  @Test
  public void saveTransIdNull(TestContext context) {
    String id = randomUuid();
    postgresClient = createFoo(context);
    AsyncResult<SQLConnection> trans = null;
    setRootLevel(Level.FATAL);
    postgresClient.save(trans, FOO, id, xPojo, context.asyncAssertFailure());
  }

  @Test
  public void startTxGetConnectionFails(TestContext context) {
    postgresClientGetConnectionFails().startTx(context.asyncAssertFailure());
  }

  @Test
  public void startTxNullConnection(TestContext context) {
    postgresClientNullConnection().startTx(context.asyncAssertFailure());
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
  public void getByIdGetConnectionFails(TestContext context) {
    postgresClientGetConnectionFails().get(FOO, StringPojo.class, "sql", true, false, context.asyncAssertFailure());
  }

  @Test
  public void getByIdNullConnection(TestContext context) {
    postgresClientNullConnection().get(FOO, StringPojo.class, "sql", true, false, context.asyncAssertFailure());
  }

  @Test
  public void getByIdUsingSqlPrimaryKey(TestContext context) {
    Async async = context.async();
    String uuid = randomUuid();
    postgresClient = createFoo(context);
    postgresClient.save(FOO, uuid, xPojo, context.asyncAssertSuccess(id -> {
      String sql = "WHERE _id='" + id + "'";
      postgresClient.get(FOO, StringPojo.class, sql, true, false, context.asyncAssertSuccess(results -> {
        try {
          assertThat(results.getResults(), hasSize(1));
          assertThat(results.getResults().get(0).key, is("x"));
          async.complete();
        } catch (Exception e) {
          context.fail(e);
        }
      }));
    }));
  }

  @Test
  public void getByIdUsingSqlPrimaryKeyWithoutUnderscore(TestContext context) {
    Async async = context.async();
    String uuid = randomUuid();
    postgresClient = createBarIdHasNoUnderscore(context);
    postgresClient.save("bar", uuid, xPojo, context.asyncAssertSuccess(id -> {
      String sql = "WHERE id='" + id + "'";
      postgresClient.get("bar", StringPojo.class, sql, true, false, context.asyncAssertSuccess(results -> {
        try {
          assertThat(results.getResults(), hasSize(1));
          assertThat(results.getResults().get(0).key, is("x"));
          async.complete();
        } catch (Exception e) {
          context.fail(e);
        }
      }));
    }));
  }

  @Test
  public void getByIdAsString(TestContext context) {
    Async async = context.async();
    String uuid = randomUuid();
    postgresClient = createFoo(context);
    postgresClient.save(FOO, uuid, xPojo, res -> {
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
    String uuid = randomUuid();
    postgresClient = createFoo(context);
    postgresClient.save(FOO, uuid, xPojo, res -> {
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
    postgresClientNonexistingTenant().getByIdAsString(
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
    postgresClient = createFoo(context);
    postgresClient.save(FOO, ids.getString(0), xPojo, res1 -> {
      assertSuccess(context, res1);
      postgresClient.save(FOO, ids.getString(1), singleQuotePojo, res2 -> {
        assertSuccess(context, res2);
        async.complete();
      });
    });
    async.awaitSuccess(5000);
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

  private PostgresClient postgresClientNonexistingTenant() {
    try {
      postgresClient = new PostgresClient(vertx, "nonexistingTenant");
      return postgresClient;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void getByIdsConnectionFailure(TestContext context) {
    postgresClientNonexistingTenant().getByIdAsString(FOO, randomUuidArray(), context.asyncAssertFailure());
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

  /**
   * @return a PostgresClient where getConnection(handler) invokes the handler with
   * a null result value and success status.
   */
  private PostgresClient postgresClientNullConnection() {
    AsyncSQLClient client = new AsyncSQLClient() {
      @Override
      public SQLClient getConnection(Handler<AsyncResult<SQLConnection>> handler) {
        handler.handle(Future.succeededFuture(null));
        return this;
      }

      @Override
      public void close(Handler<AsyncResult<Void>> handler) {
        handler.handle(Future.succeededFuture());
      }

      @Override
      public void close() {
        // nothing to do
      }
    };
    try {
      setRootLevel(Level.FATAL);
      PostgresClient postgresClient = new PostgresClient(vertx, TENANT);
      postgresClient.setClient(client);
      return postgresClient;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return a PostgresClient where getConnection(handler) invokes the handler with
   * a null result value and success status.
   */
  private PostgresClient postgresClientGetConnectionFails() {
    AsyncSQLClient client = new AsyncSQLClient() {
      @Override
      public SQLClient getConnection(Handler<AsyncResult<SQLConnection>> handler) {
        handler.handle(Future.failedFuture("postgresClientGetConnectionFails"));
        return this;
      }

      @Override
      public void close(Handler<AsyncResult<Void>> handler) {
        handler.handle(Future.succeededFuture());
      }

      @Override
      public void close() {
        // nothing to do
      }
    };
    try {
      setRootLevel(Level.FATAL);
      PostgresClient postgresClient = new PostgresClient(vertx, TENANT);
      postgresClient.setClient(client);
      return postgresClient;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return a PostgresClient where invoking SQLConnection::update(...) or SQLConnection::updateWithParams
   * throws an RuntimeException.
   */
  private PostgresClient postgresClientConnectionThrowsException() {
    SQLConnection sqlConnection = new PostgreSQLConnectionImpl(null, null, null) {
      @Override
      public SQLConnection update(String sql, Handler<AsyncResult<UpdateResult>> resultHandler) {
        throw new RuntimeException();
      }

      @Override
      public SQLConnection updateWithParams(String sql, JsonArray params,
          Handler<AsyncResult<UpdateResult>> resultHandler) {
        throw new RuntimeException();
      }

      @Override
      public void close(Handler<AsyncResult<Void>> handler) {
        handler.handle(Future.succeededFuture());
      }

      @Override
      public void close() {
        // nothing to do
      }
    };
    AsyncSQLClient client = new AsyncSQLClient() {
      @Override
      public SQLClient getConnection(Handler<AsyncResult<SQLConnection>> handler) {
        handler.handle(Future.succeededFuture(sqlConnection));
        return this;
      }

      @Override
      public void close(Handler<AsyncResult<Void>> handler) {
        handler.handle(Future.succeededFuture());
      }

      @Override
      public void close() {
        // nothing to do
      }
    };
    try {
      setRootLevel(Level.FATAL);
      PostgresClient postgresClient = new PostgresClient(vertx, TENANT);
      postgresClient.setClient(client);
      return postgresClient;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return a PostgresClient that fails when closing the connection.
   */
  private PostgresClient postgresClientEndTxFailure() {
    class PostgresClientEndTxFailure extends PostgresClient {
      public PostgresClientEndTxFailure(Vertx vertx, String tenant) throws Exception {
        super(vertx, tenant);
      }
      @Override
      public void endTx(AsyncResult<SQLConnection> conn, Handler<AsyncResult<Void>> done) {
        done.handle(Future.failedFuture(new RuntimeException()));
      }
    };
    try {
      return new PostgresClientEndTxFailure(vertx, TENANT);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void execute(TestContext context) {
    Async async = context.async();
    JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
    insertXAndSingleQuotePojo(context, ids)
    .execute("DELETE FROM tenant_raml_module_builder.foo WHERE _id='" + ids.getString(1) + "'", res -> {
      assertSuccess(context, res);
      context.assertEquals(1, res.result().getUpdated());
      async.complete();
    });
  }

  @Test
  public void executeSyntaxError(TestContext context) {
    postgresClient().execute("'", context.asyncAssertFailure());
  }

  @Test
  public void executeNullConnection(TestContext context) throws Exception {
    postgresClientNullConnection().execute("SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void executeGetConnectionFails(TestContext context) throws Exception {
    postgresClientGetConnectionFails().execute("SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void executeConnectionThrowsException(TestContext context) throws Exception {
    postgresClientConnectionThrowsException().execute("SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void executeParam(TestContext context) {
    Async async = context.async();
    JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
    insertXAndSingleQuotePojo(context, ids)
    .execute("DELETE FROM tenant_raml_module_builder.foo WHERE _id=?", new JsonArray().add(ids.getString(0)), res -> {
      assertSuccess(context, res);
      context.assertEquals(1, res.result().getUpdated());
      async.complete();
    });
  }

  @Test
  public void executeParamSyntaxError(TestContext context) {
    postgresClient().execute("'", new JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void executeParamGetConnectionFails(TestContext context) throws Exception {
    postgresClientGetConnectionFails().execute("SELECT 1", new JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void executeParamNullConnection(TestContext context) throws Exception {
    postgresClientNullConnection().execute("SELECT 1", new JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void executeParamConnectionException(TestContext context) throws Exception {
    postgresClientConnectionThrowsException().execute("SELECT 1", new JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void executeTrans(TestContext context) {
    Async async1 = context.async();
    JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
    PostgresClient postgresClient = insertXAndSingleQuotePojo(context, ids);
    postgresClient.startTx(trans -> {
      assertSuccess(context, trans);
      postgresClient.execute(trans, "DELETE FROM tenant_raml_module_builder.foo WHERE _id='" + ids.getString(1) + "'", res -> {
        assertSuccess(context, res);
        postgresClient.rollbackTx(trans, rollback -> {
          assertSuccess(context, rollback);
          async1.complete();
        });
      });
    });
    async1.awaitSuccess(5000);

    Async async2 = context.async();
    postgresClient.startTx(trans -> {
      assertSuccess(context, trans);
      postgresClient.execute(trans, "DELETE FROM tenant_raml_module_builder.foo WHERE _id='" + ids.getString(0) + "'", res -> {
        assertSuccess(context, res);
        postgresClient.endTx(trans, end -> {
          assertSuccess(context, end);
          async2.complete();
        });
      });
    });
    async2.awaitSuccess(5000);

    Async async3 = context.async();
    postgresClient.getById(FOO, ids, res -> {
      assertSuccess(context, res);
      context.assertEquals(1, res.result().size());
      async3.complete();
    });
    async3.awaitSuccess(5000);

    postgresClient.closeClient(context.asyncAssertSuccess());
  }

  @Test
  public void executeTransSyntaxError(TestContext context) {
    Async async = context.async();
    postgresClient = postgresClient();
    postgresClient.startTx(trans -> {
      postgresClient.execute(trans, "'", exec -> {
        postgresClient.rollbackTx(trans, context.asyncAssertSuccess());
        context.assertTrue(exec.failed());
        async.complete();
      });
    });
  }

  @Test
  public void executeTransNullConnection(TestContext context) throws Exception {
    setRootLevel(Level.FATAL);
    postgresClient().execute(null, "SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void executeTransParam(TestContext context) {
    Async asyncTotal = context.async();

    Async async1 = context.async();
    JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
    postgresClient = insertXAndSingleQuotePojo(context, ids);
    postgresClient.startTx(trans -> {
      assertSuccess(context, trans);
      postgresClient.execute(trans, "DELETE FROM tenant_raml_module_builder.foo WHERE _id=?", new JsonArray().add(ids.getString(1)), res -> {
        assertSuccess(context, res);
        postgresClient.rollbackTx(trans, rollback -> {
          assertSuccess(context, rollback);
          async1.complete();
        });
      });
    });
    async1.awaitSuccess(5000);

    Async async2 = context.async();
    postgresClient.startTx(trans -> {
      assertSuccess(context, trans);
      postgresClient.execute(trans, "DELETE FROM tenant_raml_module_builder.foo WHERE _id=?", new JsonArray().add(ids.getString(0)), res -> {
        assertSuccess(context, res);
        postgresClient.endTx(trans, end -> {
          assertSuccess(context, end);
          async2.complete();
        });
      });
    });
    async2.awaitSuccess(5000);

    Async async3 = context.async();
    postgresClient.getById(FOO, ids, res -> {
      assertSuccess(context, res);
      context.assertEquals(1, res.result().size());
      async3.complete();
    });
    async3.awaitSuccess(5000);

    asyncTotal.complete();
  }

  @Test
  public void executeTransParamSyntaxError(TestContext context) {
    postgresClient = postgresClient();
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.execute(trans, "'", new JsonArray(), context.asyncAssertFailure(execute -> {
        postgresClient.rollbackTx(trans, context.asyncAssertSuccess());
      }));
    }));
  }

  @Test
  public void executeTransParamNullConnection(TestContext context) throws Exception {
    Async async = context.async();
    postgresClient = postgresClient();
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      setRootLevel(Level.FATAL);
      postgresClient.execute(null, "SELECT 1", new JsonArray(), context.asyncAssertFailure(execute -> {
        postgresClient.rollbackTx(trans, rollback -> async.complete());
      }));
      // TODO: When updated to vertx 3.6.1 with this fix
      // https://github.com/vert-x3/vertx-mysql-postgresql-client/pull/132
      // change "rollback -> async.complete()" to "context.asyncAssertSuccess()"
    }));
  }

  @Test
  public void executeList(TestContext context) {
    Async async = context.async();
    JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
    List<JsonArray> list = new ArrayList<>(2);
    list.add(new JsonArray().add(ids.getString(0)));
    list.add(new JsonArray().add(ids.getString(1)));
    insertXAndSingleQuotePojo(context, ids).execute("DELETE FROM tenant_raml_module_builder.foo WHERE _id=?", list, res -> {
      assertSuccess(context, res);
      List<UpdateResult> result = res.result();
      context.assertEquals(2, result.size());
      context.assertEquals(1, result.get(0).getUpdated());
      context.assertEquals(1, result.get(1).getUpdated());
      async.complete();
    });
  }

  /** @return List containg one empty JsonArray() */
  private List<JsonArray> list1JsonArray() {
    return Collections.singletonList(new JsonArray());
  }

  @Test
  public void executeListNullConnection(TestContext context) {
    postgresClientNullConnection().execute("SELECT 1", list1JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void executeListSyntaxError(TestContext context) {
    postgresClient().execute("'", list1JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void executeListEndTxFailure(TestContext context) {
    postgresClientEndTxFailure().execute("SELECT 1", list1JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void executeListTransNull(TestContext context) throws Exception {
    setRootLevel(Level.FATAL);
    postgresClient().execute(null, "SELECT 1", list1JsonArray(), context.asyncAssertFailure());
  }

  private PostgresClient createNumbers(TestContext context, int ...numbers) {
    String schema = PostgresClient.convertToPsqlStandard(TENANT);
    execute(context, "DROP TABLE IF EXISTS numbers CASCADE;");
    execute(context, "CREATE TABLE numbers (i INT);");
    executeIgnore(context, "CREATE ROLE " + schema + " PASSWORD '" + schema + "' NOSUPERUSER NOCREATEDB INHERIT LOGIN;");
    execute(context, "GRANT ALL PRIVILEGES ON TABLE numbers TO " + schema + ";");
    StringBuilder s = new StringBuilder();
    for (int n : numbers) {
      if (s.length() > 0) {
        s.append(',');
      }
      s.append('(').append(n).append(')');
    }
    execute(context, "INSERT INTO numbers VALUES " + s + ";");
    postgresClient = postgresClient(TENANT);
    return postgresClient;
  }

  private String intsAsString(ResultSet resultSet) {
    return resultSet.getResults().stream()
        .map(jsonArray -> jsonArray.getInteger(0).toString())
        .collect(Collectors.joining(", "));
  }

  private void intsAsString(SQLRowStream sqlRowStream, Handler<AsyncResult<String>> replyHandler) {
    StringBuilder s = new StringBuilder();
    sqlRowStream.handler(row -> {
      if (s.length() > 0) {
        s.append(", ");
      }
      s.append(row.getInteger(0));
    }).exceptionHandler(e -> {
      replyHandler.handle(Future.failedFuture(e));
    }).close(close -> {
      replyHandler.handle(Future.succeededFuture(s.toString()));
    });
  }

  @Test
  public void select(TestContext context) {
    createNumbers(context, 1, 2, 3)
    .select("SELECT i FROM numbers WHERE i IN (1, 3, 5) ORDER BY i", context.asyncAssertSuccess(select -> {
      context.assertEquals("1, 3", intsAsString(select));
    }));
  }

  @Test
  public void selectTrans(TestContext context) {
    postgresClient = createNumbers(context, 4, 5, 6);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.select(trans, "SELECT i FROM numbers WHERE i IN (4, 6, 8) ORDER BY i",
          context.asyncAssertSuccess(select -> {
            postgresClient.endTx(trans, context.asyncAssertSuccess());
            context.assertEquals("4, 6", intsAsString(select));
          }));
    }));
  }

  @Test
  public void selectParam(TestContext context) {
    createNumbers(context, 7, 8, 9)
    .select("SELECT i FROM numbers WHERE i IN (?, ?, ?) ORDER BY i",
        new JsonArray().add(7).add(9).add(11), context.asyncAssertSuccess(select -> {
          context.assertEquals("7, 9",  intsAsString(select));
        }));
  }

  @Test
  public void selectParamTrans(TestContext context) {
    postgresClient = createNumbers(context, 11, 12, 13);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.select(trans, "SELECT i FROM numbers WHERE i IN (?, ?, ?) ORDER BY i",
          new JsonArray().add(11).add(13).add(15), context.asyncAssertSuccess(select -> {
            postgresClient.endTx(trans, context.asyncAssertSuccess());
            context.assertEquals("11, 13",  intsAsString(select));
          }));
    }));
  }

  @Test
  public void selectStream(TestContext context) {
    createNumbers(context, 15, 16, 17)
    .selectStream("SELECT i FROM numbers WHERE i IN (15, 17, 19) ORDER BY i", context.asyncAssertSuccess(select -> {
      intsAsString(select, context.asyncAssertSuccess(string -> {
        context.assertEquals("15, 17", string);
      }));
    }));
  }

  @Test
  public void selectStreamTrans(TestContext context) {
    postgresClient = createNumbers(context, 21, 22, 23);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.selectStream(trans, "SELECT i FROM numbers WHERE i IN (21, 23, 25) ORDER BY i",
          context.asyncAssertSuccess(select -> {
            intsAsString(select, context.asyncAssertSuccess(string -> {
              postgresClient.endTx(trans, context.asyncAssertSuccess());
              context.assertEquals("21, 23", string);
            }));
          }));
    }));
  }

  @Test
  public void selectStreamParam(TestContext context) {
    createNumbers(context, 25, 26, 27)
    .selectStream("SELECT i FROM numbers WHERE i IN (?, ?, ?) ORDER BY i",
        new JsonArray().add(25).add(27).add(29),
        context.asyncAssertSuccess(select -> {
          intsAsString(select, context.asyncAssertSuccess(string -> {
            context.assertEquals("25, 27", string);
      }));
    }));
  }

  @Test
  public void selectStreamParamTrans(TestContext context) {
    postgresClient = createNumbers(context, 31, 32, 33);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.selectStream(trans, "SELECT i FROM numbers WHERE i IN (?, ?, ?) ORDER BY i",
          new JsonArray().add(31).add(33).add(35),
          context.asyncAssertSuccess(select -> {
            intsAsString(select, context.asyncAssertSuccess(string -> {
              postgresClient.endTx(trans, context.asyncAssertSuccess());
              context.assertEquals("31, 33", string);
            }));
          }));
    }));
  }

  @Test
  public void selectSingle(TestContext context) {
    postgresClient = createNumbers(context, 41, 42, 43);
    postgresClient.selectSingle("SELECT i FROM numbers WHERE i IN (41, 43, 45) ORDER BY i",
        context.asyncAssertSuccess(select -> {
          context.assertEquals(41, select.getInteger(0));
        }));
  }

  @Test
  public void selectSingleTrans(TestContext context) {
    postgresClient = createNumbers(context, 45, 46, 47);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.selectSingle(trans, "SELECT i FROM numbers WHERE i IN (45, 47, 49) ORDER BY i",
          context.asyncAssertSuccess(select -> {
              postgresClient.endTx(trans, context.asyncAssertSuccess());
              context.assertEquals(45, select.getInteger(0));
            }));
    }));
  }

  @Test
  public void selectSingleParam(TestContext context) {
    postgresClient = createNumbers(context, 51, 52, 53);
    postgresClient.selectSingle("SELECT i FROM numbers WHERE i IN (?, ?, ?) ORDER BY i",
        new JsonArray().add(51).add(53).add(55),
        context.asyncAssertSuccess(select -> {
          context.assertEquals(51, select.getInteger(0));
        }));
  }

  @Test
  public void selectSingleParamTrans(TestContext context) {
    postgresClient = createNumbers(context, 55, 56, 57);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.selectSingle(trans, "SELECT i FROM numbers WHERE i IN (?, ?, ?) ORDER BY i",
          new JsonArray().add(51).add(53).add(55),
          context.asyncAssertSuccess(select -> {
              postgresClient.endTx(trans, context.asyncAssertSuccess());
              context.assertEquals(55, select.getInteger(0));
            }));
    }));
  }

  @Test
  public void selectTxException(TestContext context) {
    postgresClient().select(null, "SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void selectParamTxException(TestContext context) {
    postgresClient().select(null, "SELECT 1", new JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void selectSingleTxException(TestContext context) {
    postgresClient().selectSingle(null, "SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void selectSingleParamTxException(TestContext context) {
    postgresClient().selectSingle(null, "SELECT 1", new JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void selectStreamTxException(TestContext context) {
    postgresClient().selectStream(null, "SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void selectStreamParamTxException(TestContext context) {
    postgresClient().selectStream(null, "SELECT 1", new JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void selectTxFailed(TestContext context) {
    postgresClient().select(Future.failedFuture("failed"), "SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void selectParamTxFailed(TestContext context) {
    postgresClient().select(Future.failedFuture("failed"), "SELECT 1", new JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void selectSingleTxFailed(TestContext context) {
    postgresClient().selectSingle(Future.failedFuture("failed"), "SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void selectSingleParamTxFailed(TestContext context) {
    postgresClient().selectSingle(Future.failedFuture("failed"), "SELECT 1", new JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void selectStreamTxFailed(TestContext context) {
    postgresClient().selectStream(Future.failedFuture("failed"), "SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void selectStreamParamTxFailed(TestContext context) {
    postgresClient().selectStream(Future.failedFuture("failed"), "SELECT 1", new JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void selectDistinctOn(TestContext context) throws IOException {
    Async async = context.async();
    final String tableDefiniton = "_id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";
    postgresClient = createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);

    postgresClient.select("SELECT DISTINCT ON (jsonb->>'owner') * FROM mock_po_lines  ORDER BY (jsonb->>'owner') DESC", select -> {
      context.assertEquals(3, select.result().getResults().size());
      async.complete();
    });
    async.awaitSuccess();
  }

  @Test
  public void streamGetDistinctOn(TestContext context) throws IOException {
    AtomicInteger objectCount = new AtomicInteger();
    Async async = context.async();

    final String tableDefiniton = "_id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";

    postgresClient = createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", "", false, false,
      "jsonb->>'edition'", streamHandler -> objectCount.incrementAndGet(), asyncResult -> {
        context.assertEquals(2, objectCount.get());
        async.complete();
      });
    async.awaitSuccess();
  }

  // While facets are passed, this does NOT seem to deal with facets
  // In fact, quite possibly, streamGet do not support facets at all
  @Test
  public void streamGetDistinctOnWithFacets(TestContext context) throws IOException {
    AtomicInteger objectCount = new AtomicInteger();
    Async async = context.async();

    final String tableDefiniton = "_id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";

    List<FacetField> facets = new ArrayList<FacetField>() {{
      add(new FacetField("jsonb->>'edition'"));
    }};

    postgresClient = createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);

    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", "", false, false,
      facets,"jsonb->>'edition'", streamHandler -> objectCount.incrementAndGet(), asyncResult -> {
        context.assertEquals(2, objectCount.get());
        async.complete();
      });
    async.awaitSuccess();
  }

  @Test
  public void getDistinctOn(TestContext context) throws IOException {
    Async async = context.async();
    final String tableDefiniton = "_id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";
    postgresClient = createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);

    String distinctOn = "jsonb->>'order_format'";
    postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*", "", false, false,
      false, null, distinctOn, handler -> {
        ResultInfo resultInfo = handler.result().getResultInfo();
        context.assertEquals(4, resultInfo.getTotalRecords());
        async.complete();
      });
    async.awaitSuccess();

    String whereClause =  "WHERE jsonb->>'order_format' = 'Other'";
    Async async2 = context.async();
    postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*", whereClause, false, false,
      false, null, distinctOn, handler -> {
        ResultInfo resultInfo = handler.result().getResultInfo();
        context.assertEquals(1, resultInfo.getTotalRecords());
        async2.complete();
      });
    async2.awaitSuccess();
  }

  @Test
  public void getDistinctOnWithFacets(TestContext context) throws IOException {
    final String tableDefiniton = "_id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";
    postgresClient = createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);

    List<FacetField> facets = new ArrayList<FacetField>() {{
      add(new FacetField("jsonb->>'edition'"));
    }};
    String distinctOn = "jsonb->>'order_format'";
    //with facets and return count
    Async async1 = context.async();
    postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*", "", true, false,
      false, facets, distinctOn, handler -> {
        ResultInfo resultInfo = handler.result().getResultInfo();
        context.assertEquals(4, resultInfo.getTotalRecords());
        List<Facet> retFacets = resultInfo.getFacets();
        context.assertEquals(1, retFacets.size());
        async1.complete();
      });
    async1.awaitSuccess();

    String whereClause =  "WHERE jsonb->>'order_format' = 'Other'";

    //with facets and where clause RMB-355
    Async async2 = context.async();
    postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*", whereClause, true, false,
      false, facets, distinctOn, handler -> {
        ResultInfo resultInfo = handler.result().getResultInfo();
        context.assertEquals(1, resultInfo.getTotalRecords());
        List<Facet> retFacets = resultInfo.getFacets();
        context.assertEquals(1, retFacets.size());
        async2.complete();
      });
    async2.awaitSuccess();
  }

  @Test(expected = Exception.class)
  public void pojo2jsonNull() throws Exception {
    PostgresClient.pojo2json(null);
  }

  @Test
  public void pojo2jsonJson(TestContext context) throws Exception {
    JsonObject j = new JsonObject().put("a", "b");
    context.assertEquals("{\"a\":\"b\"}", PostgresClient.pojo2json(j));
  }

  @Test
  public void pojo2jsonMap(TestContext context) throws Exception {
    Map<String,String> m = new HashMap<>();
    m.put("a", "b");
    context.assertEquals("{\"a\":\"b\"}", PostgresClient.pojo2json(m));
  }

  @Test(expected = Exception.class)
  public void pojo2jsonBadMap(TestContext context) throws Exception {
    PostgresClient.pojo2json(postgresClient);
  }

  private PostgresClient createTableWithPoLines(TestContext context, String tableName, String tableDefiniton) throws IOException {
    String schema = PostgresClient.convertToPsqlStandard(TENANT);
    String polines = getMockData("mockdata/poLines.json");
    postgresClient = createTable(context, TENANT, tableName, tableDefiniton);

    // get count_estimate_smart2 definition
    Async async = context.async();
    try {
      String sql = IOUtils.toString(
        getClass().getClassLoader().getResourceAsStream("templates/db_scripts/funcs.sql"), "UTF-8");
      sql = sql.replaceAll("tenants_raml_module_builder.", "");
      postgresClient.getClient().update(sql, reply -> {
        assertSuccess(context, reply);
        async.complete();
      });
    } catch (IOException ex) {
      log.error("createTable: " + ex.getMessage());
      async.complete();
    }
    async.awaitSuccess(1000);

    for (String jsonbValue : polines.split("\n")) {
      String additionalField = new JsonObject(jsonbValue).getString("publication_date");
      execute(context, "INSERT INTO " + schema + "." + tableName + " (_id, jsonb, distinct_test_field) VALUES "
        + "('" + randomUuid() + "', '" + jsonbValue + "' ," + additionalField + " ) ON CONFLICT DO NOTHING;");
    }
    return postgresClient;
  }

  public static String getMockData(String path) throws IOException {
    try (InputStream resourceAsStream = PostgresClientIT.class.getClassLoader().getResourceAsStream(path)) {
      if (resourceAsStream != null) {
        return IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8);
      } else {
        StringBuilder sb = new StringBuilder();
        try (Stream<String> lines = Files.lines(Paths.get(path))) {
          lines.forEach(sb::append);
        }
        return sb.toString();
      }
    }
  }
}
