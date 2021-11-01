package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgNotification;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.RowImpl;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.TransactionRollbackException;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.impl.RowDesc;
import io.vertx.sqlclient.spi.DatabaseMetadata;
import org.apache.commons.io.IOUtils;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.CQL2PgJSONException;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.jaxrs.model.Facet;
import org.folio.rest.jaxrs.model.ResultInfo;
import org.folio.rest.persist.PostgresClient.QueryHelper;
import org.folio.rest.persist.PostgresClient.TotaledResults;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.Criteria.UpdateSection;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.facets.FacetField;
import org.folio.rest.persist.helpers.LocalRowSet;
import org.folio.rest.persist.helpers.Poline;
import org.folio.rest.persist.helpers.SimplePojo;
import org.folio.rest.persist.interfaces.Results;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class PostgresClientIT {
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

  private PostgresClient postgresClient;

  @Rule
  public Timeout rule = Timeout.seconds(15);

  private int QUERY_TIMEOUT = 0;

  @BeforeClass
  public static void doesNotCompleteOnWindows() {
    final String os = System.getProperty("os.name").toLowerCase();
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    org.junit.Assume.assumeFalse(os.contains("win")); // RMB-261
  }

  @BeforeClass
  public static void setUpClass(TestContext context) throws Exception {
    vertx = VertxUtils.getVertxWithExceptionHandler();

    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    PostgresClient.setExplainQueryThreshold(0);

    // fail the complete test class if the connection to postgres doesn't work
    PostgresClient.getInstance(vertx).execute("SELECT 1", context.asyncAssertSuccess());
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    PostgresClient.stopPostgresTester();
    PostgresClient.setExplainQueryThreshold(PostgresClient.EXPLAIN_QUERY_THRESHOLD_DEFAULT);
    if (vertx != null) {
      vertx.close(context.asyncAssertSuccess());
      vertx = null;
    }
  }

  @Before
  public void setUp() {
    postgresClient = null;
  }

  @After
  public void tearDown(TestContext context) {
    if (postgresClient != null) {
      postgresClient.closeClient(context.asyncAssertSuccess());
      postgresClient = null;
    }
    PostgresClient.sharedPgPool = false;
  }

  private static <T> void assertSuccess(TestContext context, AsyncResult<T> result) {
    if (result.failed()) {
      context.fail(result.cause());
    }
  }

  /**
   * Similar to context.asyncAssertSuccess(resultHandler) but the type of the resultHandler
   * is {@code Handler<AsyncResult<SQLConnection>>} and not {@code Handler<SQLConnection>}.
   * Usage: {@code postgresClient.startTx(asyncAssertTx(context, trans ->}
   */
  private static Handler<AsyncResult<SQLConnection>> asyncAssertTx(
      TestContext context, Handler<AsyncResult<SQLConnection>> resultHandler) {

    Async async = context.async();
    return trans -> {
      if (trans.failed()) {
        context.fail(trans.cause());
      }
      resultHandler.handle(trans);
      async.complete();
    };
  }

  @Test
  public void closeClient(TestContext context) {
    PostgresClient c = PostgresClient.getInstance(vertx);
    context.assertNotNull(PostgresClientHelper.getClient(c), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(PostgresClientHelper.getClient(c), "getClient()");
  }

  @Test
  public void closeClientTenant(TestContext context) {
    PostgresClient c = PostgresClient.getInstance(vertx, TENANT);
    context.assertNotNull(PostgresClientHelper.getClient(c), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(PostgresClientHelper.getClient(c), "getClient()");
  }

  @Test
  public void closeClientTwice(TestContext context) {
    PostgresClient c = PostgresClient.getInstance(vertx);
    context.assertNotNull(PostgresClientHelper.getClient(c), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(PostgresClientHelper.getClient(c), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(PostgresClientHelper.getClient(c), "getClient()");
  }

  @Test
  public void closeClientTwiceTenant(TestContext context) {
    PostgresClient c = PostgresClient.getInstance(vertx, TENANT);
    context.assertNotNull(PostgresClientHelper.getClient(c), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(PostgresClientHelper.getClient(c), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(PostgresClientHelper.getClient(c), "getClient()");
  }

  @Test
  public void closeClientGetInstance(TestContext context) {
    PostgresClient c = PostgresClient.getInstance(vertx, TENANT);
    context.assertNotNull(PostgresClientHelper.getClient(c), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(PostgresClientHelper.getClient(c), "getClient()");
    c = PostgresClient.getInstance(vertx, TENANT);
    context.assertNotNull(PostgresClientHelper.getClient(c), "getClient()");
    c.closeClient(context.asyncAssertSuccess());
    context.assertNull(PostgresClientHelper.getClient(c), "getClient()");
  }

  @Test
  public void closeAllClients(TestContext context) {
    PostgresClient c = PostgresClient.getInstance(vertx);
    context.assertNotNull(PostgresClientHelper.getClient(c), "getClient()");
    assertThat(PostgresClient.getConnectionPoolSize(), is(not(0)));
    PostgresClient.closeAllClients();
    assertThat(PostgresClient.getConnectionPoolSize(), is(0));
  }

  @Test
  public void closeAllClientsTenant(TestContext context) {
    PostgresClient.closeAllClients();
    PostgresClient.getInstance(vertx, "a");
    PostgresClient.getInstance(Vertx.vertx(), "a");
    PostgresClient.getInstance(vertx, "b");
    assertThat(PostgresClient.getConnectionPoolSize(), is(3));
    PostgresClient.closeAllClients("c");
    assertThat(PostgresClient.getConnectionPoolSize(), is(3));
    PostgresClient.closeAllClients("b");
    assertThat(PostgresClient.getConnectionPoolSize(), is(2));
    PostgresClient.closeAllClients("a");
    assertThat(PostgresClient.getConnectionPoolSize(), is(0));
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

  private void assertPoolsOfTwoTenants(TestContext context, boolean shared, int expectedPools) {
    PostgresClient.closeAllClients();
    PostgresClient.sharedPgPool = shared;
    PostgresClient c1 = createA(context, "t1");
    PostgresClient c2 = createA(context, "t2");
    c1.execute("INSERT INTO a VALUES (1)")
    .compose(x -> c2.execute("SELECT i FROM a"))
    .onComplete(context.asyncAssertSuccess(rowSet -> {
      assertThat(rowSet.size(), is(0));
    }))
    .compose(x -> c1.execute("SELECT i FROM a"))
    .onComplete(context.asyncAssertSuccess(rowSet -> {
      assertThat(rowSet.size(), is(1));
      assertThat(PostgresClient.getConnectionPoolSize(), is(expectedPools));
    }))
    .compose(x -> {
      // DROP ROLE only succeeds if we switch back from t1 ROLE to a superuser ROLE
      String sql = "DROP SCHEMA t1_raml_module_builder CASCADE;"
          + "DROP ROLE t1_raml_module_builder";
      return PostgresClient.getInstance(vertx).execute(sql);
    })
    .onComplete(context.asyncAssertSuccess(x -> {
      PostgresClient.closeAllClients();
    }));
  }

  @Test
  public void getNewInstancesSeparatePools(TestContext context) {
    // 1 for default schema public, 1 for t1, 1 for t2
    assertPoolsOfTwoTenants(context, false, 3);
  }

  @Test
  public void getNewInstanceSharedPool(TestContext context) {
    assertPoolsOfTwoTenants(context, true, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void instanceDefaultSchema(TestContext context) {
    PostgresClient.getInstance(vertx, PostgresClient.DEFAULT_SCHEMA);
  }

  private void execute(TestContext context, String sql) {
    Async async = context.async();
    PostgresClient c = PostgresClient.getInstance(vertx);
    c.execute(sql, context.asyncAssertSuccess(reply -> async.complete()));
    async.awaitSuccess(5000);
  }

  private void executeIgnore(TestContext context, String sql) {
    Async async = context.async();
    PostgresClient c = PostgresClient.getInstance(vertx);
    c.execute(sql, reply -> async.complete());
    async.awaitSuccess(5000);
  }

  private PostgresClient postgresClient(String tenant) {
    try {
      postgresClient = PostgresClient.getInstance(vertx, tenant);
    } catch (Throwable e) {
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
    execute(context, "DROP SCHEMA IF EXISTS " + schema + " CASCADE;");
    executeIgnore(context, "CREATE ROLE " + schema + " PASSWORD '" + tenant + "' NOSUPERUSER NOCREATEDB INHERIT LOGIN;");
    execute(context, "CREATE SCHEMA " + schema + " AUTHORIZATION " + schema);
    execute(context, "GRANT ALL PRIVILEGES ON SCHEMA " + schema + " TO " + schema);
    LoadGeneralFunctions.loadFuncs(context, PostgresClient.getInstance(vertx), schema);
    execute(context, "CREATE TABLE " + schema + "." + table + " (" + tableDefinition + ");");
    execute(context, "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA " + schema + " TO " + schema);
    return postgresClient(tenant);
  }

  /** create table a (i INTEGER) */
  private PostgresClient createA(TestContext context, String tenant) {
    return createTable(context, tenant, "a", "i INTEGER");
  }

  private PostgresClient createFoo(TestContext context) {
    PostgresClient postgresClient = createTable(context, TENANT, FOO,
        "id UUID PRIMARY KEY , jsonb JSONB NOT NULL");
    String schema = PostgresClient.convertToPsqlStandard(TENANT);
    execute(context, "CREATE TRIGGER set_id_in_jsonb BEFORE INSERT OR UPDATE ON " + schema + "."  + FOO +
        " FOR EACH ROW EXECUTE PROCEDURE " + schema + ".set_id_in_jsonb();");
    return postgresClient;
  }

  private PostgresClient createFooBinary(TestContext context) {
    return createTable(context, TENANT, FOO,
        "id UUID PRIMARY KEY , jsonb TEXT NOT NULL");  // TEXT to store binary data
  }

  private PostgresClient createInvalidJson(TestContext context) {
    String schema = PostgresClient.convertToPsqlStandard(TENANT);
    postgresClient = createTable(context, TENANT, INVALID_JSON,
        "id UUID PRIMARY KEY, jsonb VARCHAR(99) NOT NULL");
    execute(context, "INSERT INTO " + schema + "." + INVALID_JSON + " VALUES "
        +"('" + INVALID_JSON_UUID + "', '}');");
    return postgresClient;
  }

  private void fillA(TestContext context, PostgresClient client, String tenant, int i) {
    Async async = context.async();
    String schema = PostgresClient.convertToPsqlStandard(tenant);
    execute(context, "INSERT INTO "  + schema + ".a (i) VALUES (" + i + ") ON CONFLICT DO NOTHING;");
    client.select("SELECT i FROM " + schema + ".a", context.asyncAssertSuccess(get -> {
      context.assertEquals(i, get.iterator().next().getInteger(0));
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

  /**
   * Each connection runs pg_sleep(1) that takes 1 second.
   * Setting maxPoolSize=90 runs them in parallel so that it
   * completes within 5 seconds.
   *
   * <p>If you want to test with a higher maxPoolSize you
   * need to increase max_connections so that maxPoolSize is within
   * (max_connections - superuser_reserved_connections):
   * <a href="https://www.postgresql.org/docs/current/runtime-config-connection.html">
   * https://www.postgresql.org/docs/current/runtime-config-connection.html</a>
   */
  @Test(timeout = 5000)  // milliseconds
  public void maxPoolSize(TestContext context) {
    int maxPoolSize = 90;
    postgresClient = createA(context, TENANT);
    JsonObject configuration = postgresClient.getConnectionConfig().copy()
        .put("maxPoolSize", maxPoolSize);
    postgresClient.setClient(PostgresClient.createPgPool(vertx, configuration));
    List<Future> futures = new ArrayList<>();
    for (int i=0; i<maxPoolSize; i++) {
      futures.add(Future.<RowSet<Row>>future(promise ->
        postgresClient.execute("SELECT pg_sleep(1)", promise)));
    }
    CompositeFuture.all(futures).onComplete(context.asyncAssertSuccess());
  }

  public static class StringPojo {
    public String id;
    public String key;
    public StringPojo() {
      // required by ObjectMapper.readValue for JSON to POJO conversion
    }
    public StringPojo(String key) {
      this.key = key;
    }
    public StringPojo(String key, String id) {
      this.key = key;
      this.id = id;
    }
    public String getId() {
      return id;
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
  public void deleteById(TestContext context) {
    String id = randomUuid();
    String id2 = randomUuid();
    PostgresClient postgresClient = insertXAndSingleQuotePojo(context, new JsonArray().add(id).add(id2));
    postgresClient.delete(FOO, id, context.asyncAssertSuccess(delete -> {
      context.assertEquals(1, delete.rowCount(), "number of records deleted");
      postgresClient.selectSingle("SELECT count(*) FROM " + FOO, context.asyncAssertSuccess(select -> {
        context.assertEquals(1, select.getInteger(0), "remaining records");
        postgresClient.delete(FOO, id, context.asyncAssertSuccess(delete2 -> {
          context.assertEquals(0, delete2.rowCount(), "number of records deleted");
        }));
      }));
    }));
  }

  @Test
  public void deleteByIdFailure(TestContext context) {
    createFoo(context).delete(Future.failedFuture("nada"), FOO, randomUuid(), context.asyncAssertFailure(delete -> {
      context.assertEquals("nada", delete.getMessage());
    }));
  }

  @Test
  public void deleteByIdNullConnection(TestContext context) {
    createFoo(context).delete(null, FOO, randomUuid(), context.asyncAssertFailure(fail -> {
      context.assertTrue(fail instanceof NullPointerException);
    }));
  }

  @Test
  public void deleteByIdException(TestContext context) {
    postgresClientConnectionThrowsException().delete(FOO, randomUuid(), context.asyncAssertFailure(e -> {
      assertThat(e, is(instanceOf(RuntimeException.class)));
    }));
  }

  private void deleteByCqlWrapper(TestContext context, String key) throws FieldException {
    Async async = context.async();
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("jsonb");
    CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, "key==" + key);
    PostgresClient postgresClient = insertXAndSingleQuotePojo(context, new JsonArray().add(randomUuid()).add(randomUuid()));
    postgresClient.delete(FOO, cqlWrapper, context.asyncAssertSuccess(delete -> {
      context.assertEquals(1, delete.rowCount(), "number of records deleted");
      postgresClient.selectSingle("SELECT count(*) FROM " + FOO, context.asyncAssertSuccess(select -> {
        context.assertEquals(1, select.getInteger(0), "remaining records");
        async.complete();
      }));
    }));
    async.await(5000);
  }

  @Test
  public void deleteByCqlWrapper(TestContext context) throws FieldException {
    deleteByCqlWrapper(context, "x");
    deleteByCqlWrapper(context, "'");  // SQL injection?
  }

  @Test
  public void deleteByCqlWrapperThatThrowsException(TestContext context) {
    CQLWrapper cqlWrapper = new CQLWrapper() {
      @Override
      public String getWhereClause() {
        throw new RuntimeException("ping pong");
      }
    };
    createFoo(context).getSQLConnection(asyncAssertTx(context, sqlConnection  -> {
      postgresClient.delete(sqlConnection,  FOO, cqlWrapper, context.asyncAssertFailure(fail -> {
        context.assertTrue(fail.getMessage().contains("ping pong"));
      }));
    }));
  }

  private void deleteByCriterion(TestContext context, String key) throws FieldException {
    Async async = context.async();
    Criterion criterion = new Criterion();
    criterion.addCriterion(new Criteria().addField("'key'").setOperation("=").setVal(key));
    PostgresClient postgresClient = insertXAndSingleQuotePojo(context, new JsonArray().add(randomUuid()).add(randomUuid()));
    postgresClient.delete(FOO, criterion, context.asyncAssertSuccess(delete -> {
      context.assertEquals(1, delete.rowCount(), "number of records deleted");
      postgresClient.selectSingle("SELECT count(*) FROM " + FOO, context.asyncAssertSuccess(select -> {
        context.assertEquals(1, select.getInteger(0), "remaining records");
        async.complete();
      }));
    }));
    async.await(5000);
  }

  @Test
  public void deleteByCriterionX(TestContext context) throws FieldException {
    deleteByCriterion(context, "x");
  }

  @Test
  public void deleteByCriterionSingleQuote(TestContext context) throws FieldException {
    deleteByCriterion(context, "'");  // SQL injection?
  }

  @Test
  public void deleteByCriterionThatThrowsException(TestContext context) {
    Criterion criterion = new Criterion() {
      public String toString() {
        throw new RuntimeException("missing towel");
      }
    };
    createFoo(context).delete(FOO, criterion, context.asyncAssertFailure(fail -> {
      context.assertTrue(fail.getMessage().contains("missing towel"));
    }));
  }

  @Test
  public void deleteByCriterionFailedConnection(TestContext context) {
    createFoo(context).delete(Future.failedFuture("okapi"), FOO, new Criterion(), context.asyncAssertFailure(fail -> {
      context.assertTrue(fail.getMessage().contains("okapi"));
    }));
  }

  @Test
  public void deleteByCriterionNullConnection(TestContext context) {
    createFoo(context).delete(null, FOO, new Criterion(), context.asyncAssertFailure(fail -> {
      context.assertTrue(fail instanceof NullPointerException);
    }));
  }

  @Test
  public void deleteByCriterionDeleteFails(TestContext context) {
    postgresClientQueryFails().delete(FOO, new Criterion(), context.asyncAssertFailure(fail -> {
      context.assertTrue(fail.getMessage().contains("queryFails"));
    }));
  }

  @Test
  public void deleteByPojo(TestContext context) throws FieldException {
    StringPojo pojo1 = new StringPojo("'", randomUuid());
    StringPojo pojo2 = new StringPojo("x", randomUuid());
    PostgresClient postgresClient = insertXAndSingleQuotePojo(context, new JsonArray().add(pojo2.id).add(pojo1.id));
    postgresClient.delete(FOO, pojo2, context.asyncAssertSuccess(delete1 -> {
      context.assertEquals(1, delete1.rowCount(), "number of records deleted");
      postgresClient.selectSingle("SELECT count(*) FROM " + FOO, context.asyncAssertSuccess(select1 -> {
        context.assertEquals(1, select1.getInteger(0), "remaining records");
        postgresClient.delete(FOO, pojo1, context.asyncAssertSuccess(delete2 -> {
          context.assertEquals(1, delete2.rowCount(), "number of records deleted");
          postgresClient.selectSingle("SELECT count(*) FROM " + FOO, context.asyncAssertSuccess(select2 -> {
            context.assertEquals(0, select2.getInteger(0), "remaining records");
          }));
        }));
      }));
    }));
  }

  @Test
  public void deleteByPojoFailedConnection(TestContext context) throws FieldException {
    createFoo(context).delete(Future.failedFuture("bad"), FOO, new SimplePojo(), context.asyncAssertFailure());
  }

  @Test
  public void deleteByPojoNullEntity(TestContext context) throws FieldException {
    createFoo(context).delete(FOO, (SimplePojo) null, context.asyncAssertFailure());
  }

  @Test
  public void deleteByPojoDeleteFails(TestContext context) throws FieldException {
    postgresClientQueryFails().delete(FOO, new SimplePojo(), context.asyncAssertFailure());
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
  public void updateIdWithSingleQuote(TestContext context) {
    createFoo(context)
      .update(FOO, xPojo, "foo'bar", context.asyncAssertFailure(fail -> {
        assertThat(fail.getMessage(), containsString("syntax"));  // invalid input syntax for type uuid
        // we don't want SQL injection with 42601 syntax error
      }));
  }

  @Test
  public void updateWithCriterion(TestContext context) {
    Criterion criterion = new Criterion().addCriterion(new Criteria()
        .addField("'key'").setOperation("=").setVal("x"));
    createFoo(context).save(FOO, xPojo, context.asyncAssertSuccess(save -> {
      postgresClient.update(FOO, singleQuotePojo, criterion, true, context.asyncAssertSuccess(rowSet -> {
        assertThat(rowSet.rowCount(), is(1));
        postgresClient.update(FOO, xPojo, criterion, true, context.asyncAssertSuccess(rowSet2 -> {
          assertThat(rowSet2.rowCount(), is(0));
        }));
      }));
    }));
  }

  @Test
  public void updateWithCqlWrapper(TestContext context) throws Exception {
    CQLWrapper cqlWrapper = new CQLWrapper(new CQL2PgJSON("jsonb"), "key=x");
    createFoo(context).save(FOO, xPojo, context.asyncAssertSuccess(save -> {
      postgresClient.update(FOO, singleQuotePojo, cqlWrapper, true, context.asyncAssertSuccess(rowSet -> {
        assertThat(rowSet.rowCount(), is(1));
        postgresClient.update(FOO, xPojo, cqlWrapper, true, context.asyncAssertSuccess(rowSet2 -> {
          assertThat(rowSet2.rowCount(), is(0));
        }));
      }));
    }));
  }

  @Test
  public void updateWithWhereClause(TestContext context) throws Exception {
    String where = "WHERE jsonb->>'key'='x'";
    createFoo(context).save(FOO, xPojo, context.asyncAssertSuccess(save -> {
      postgresClient.update(FOO, singleQuotePojo, "jsonb", where, true, context.asyncAssertSuccess(rowSet -> {
        assertThat(rowSet.rowCount(), is(1));
        postgresClient.update(FOO, xPojo, "jsonb", where, true, context.asyncAssertSuccess(rowSet2 -> {
          assertThat(rowSet2.rowCount(), is(0));
        }));
      }));
    }));
  }

  @Test
  public void updateWithWhereClauseSqlConnection(TestContext context) throws Exception {
    String where = "WHERE jsonb->>'key'='x'";
    createFoo(context).save(FOO, xPojo, context.asyncAssertSuccess(save -> {
      postgresClient.getSQLConnection(conn -> {
        postgresClient.update(conn, FOO, singleQuotePojo, "jsonb", where, true, context.asyncAssertSuccess(rowSet -> {
          assertThat(rowSet.rowCount(), is(1));
          postgresClient.update(conn, FOO, xPojo, "jsonb", where, true, context.asyncAssertSuccess(rowSet2 -> {
            assertThat(rowSet2.rowCount(), is(0));
          }));
        }));
      });
    }));
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

  @Test
  public void updateSectionSingleQuote(TestContext context) {
    UpdateSection updateSection = new UpdateSection();
    updateSection.addField("key").setValue("'");

    postgresClient = createFoo(context);
    postgresClient.save(FOO, xPojo, context.asyncAssertSuccess(save -> {
      postgresClient.update(FOO, updateSection, (Criterion) null, true, context.asyncAssertSuccess(update -> {
        context.assertEquals(1, update.rowCount(), "number of records updated");
        postgresClient.selectSingle("SELECT jsonb->>'key' FROM " + FOO, context.asyncAssertSuccess(select -> {
          context.assertEquals("'", select.getString(0), "single quote");
        }));
      }));
    }));
  }

  @Test
  public void sqlConnectionWithTimeoutSuccess(TestContext context) {
    PostgresClient client = postgresClient();
    client.getSQLConnection(2000, asyncAssertTx(context, conn -> {
      client.selectSingle(conn, "SELECT 1, pg_sleep(1);",
          client.closeAndHandleResult(conn, context.asyncAssertSuccess()));
    }));
  }

  @Test
  public void sqlConnectionWithTimeoutFailure(TestContext context) {
    PostgresClient client = postgresClient();
    client.getSQLConnection(500, asyncAssertTx(context, conn -> {
      client.selectSingle(conn, "SELECT 1, pg_sleep(3);",
          client.closeAndHandleResult(conn, context.asyncAssertFailure(e -> {
            String sqlState = new PgExceptionFacade(e).getSqlState();
            assertThat(PgExceptionUtil.getMessage(e), sqlState, is("57014"));  // query_canceled
          })));
    }));
  }

  @Test
  public void transWithError(TestContext context) {
    postgresClient().withTrans(trans -> {
      return trans.getPgConnection().query("SELECT (").execute();
    }).onComplete(context.asyncAssertFailure(select -> {
      assertThat(new PgExceptionFacade(select).getSqlState(), is("42601")); // syntax error
    }));
  }

  @Test
  public void transWithTimeoutSuccess(TestContext context) {
    postgresClient().withTrans(3000, trans -> {
      return trans.getPgConnection().query("SELECT 1, pg_sleep(0.1)").execute();
    }).onComplete(context.asyncAssertSuccess(rowSet -> {
      assertThat(rowSet.iterator().next().getInteger(0), is(1));
    }));
  }

  @Test
  public void transWithTimeoutFailure(TestContext context) {
    postgresClient().withTrans(1, trans -> {
      return trans.getPgConnection().query("SELECT 1, pg_sleep(3)").execute();
    }).onComplete(context.asyncAssertFailure(e -> {
      assertThat(e.getMessage(), containsString("57014"));  // query_canceled
    }));
  }

  @Test
  public void updateSectionCriterion(TestContext context) {
    // update key=z where key='
    Criterion criterion = new Criterion().addCriterion(new Criteria().addField("'key'").setOperation("=").setVal("'"));
    UpdateSection updateSection = new UpdateSection();
    updateSection.addField("key").setValue("z");

    String id = randomUuid();
    postgresClient = insertXAndSingleQuotePojo(context, new JsonArray().add(randomUuid()).add(id));
    postgresClient.update(FOO, updateSection, criterion, false, context.asyncAssertSuccess(update -> {
      context.assertEquals(1, update.rowCount(), "number of records updated");
      String sql = "SELECT jsonb->>'key' FROM " + FOO + " WHERE id='"+id+"'";
      postgresClient.selectSingle(sql, context.asyncAssertSuccess(select -> {
        context.assertEquals("z", select.getString(0), "single quote became z");
      }));
    }));
  }

  @Test
  public void updateSectionException(TestContext context) {
    createFoo(context).update(FOO, (UpdateSection)null, (Criterion)null, false, context.asyncAssertFailure());
  }

  @Test
  public void updateSectionFailure(TestContext context) {
    UpdateSection updateSection = new UpdateSection();
    updateSection.addField("key").setValue("x");
    createFoo(context).update("nonexistingTable", updateSection, (Criterion)null, false, context.asyncAssertFailure());
  }

  @Test
  public void getByIdBadUUID(TestContext context) {
    postgresClient = createFoo(context);
      postgresClient.getById(FOO, "bad-uid", context.asyncAssertFailure(res -> {
        context.assertTrue(res.getMessage().contains("Invalid UUID"));
      }));
  }

  @Test
  public void save3(TestContext context) {
    postgresClient = createFoo(context);
    postgresClient.save(FOO, xPojo, context.asyncAssertSuccess(save -> {
      String id = save;
      postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
        context.assertEquals("x", get.getString("key"));
      }));
    }));
  }

  @Test
  public void save4returnId(TestContext context) {
    postgresClient = createFoo(context);
    postgresClient.save(FOO, xPojo, /* returnId */ true, context.asyncAssertSuccess(save -> {
      String id = save;
      postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
        context.assertEquals("x", get.getString("key"));
      }));
    }));
  }

  @Test
  public void save4doNotReturnId(TestContext context) {
    createFoo(context).save(FOO, xPojo, /* returnId */ false, context.asyncAssertSuccess(save -> {
      context.assertEquals("", save);
    }));
  }

  @Test
  public void save4id(TestContext context) {
    String id = randomUuid();
    postgresClient = createFoo(context);
    postgresClient.save(FOO, id, xPojo, context.asyncAssertSuccess(save -> {
      postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
        context.assertEquals("x", get.getString("key"));
      }));
    }));
  }

  @Test
  public void save4idNull(TestContext context) {
    postgresClient = createFoo(context);
    postgresClient.save(FOO, /* id */ null, xPojo, context.asyncAssertSuccess(save -> {
      String id = save;
      postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
        context.assertEquals("x", get.getString("key"));
      }));
    }));
  }

  @Test
  public void save5returnId(TestContext context) {
    String id = randomUuid();
    createFoo(context).save(FOO, id, xPojo, /* returnId */ true, context.asyncAssertSuccess(save -> {
      context.assertEquals(id, save);
      postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
        context.assertEquals("x", get.getString("key"));
      }));
    }));
  }

  @Test
  public void save5doNotReturnId(TestContext context) {
    String id = randomUuid();
    createFoo(context).save(FOO, id, xPojo, /* returnId */ false, context.asyncAssertSuccess(save -> {
      context.assertEquals("", save);
      postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
        context.assertEquals("x", get.getString("key"));
      }));
    }));
  }

  @Test
  public void save6upsert(TestContext context) {
    String id = randomUuid();
    PostgresClient postgresClient = createFoo(context);
    postgresClient.save(FOO, id, xPojo, /* returnId */ true, /* upsert */ true, context.asyncAssertSuccess(save -> {
      context.assertEquals(id, save);
      postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
        context.assertEquals("x", get.getString("key"));
        postgresClient.save(FOO, id, singleQuotePojo, /* returnId */ true, /* upsert */ true, context.asyncAssertSuccess(update -> {
          context.assertEquals(id, update);
          postgresClient.getById(FOO, id, context.asyncAssertSuccess(get2 -> {
            context.assertEquals("'", get2.getString("key"));
          }));
        }));
      }));
    }));
  }

  @Test
  public void save6noUpsert(TestContext context) {
    String id = randomUuid();
    PostgresClient postgresClient = createFoo(context);
    postgresClient.save(FOO, id, xPojo, /* returnId */ true, /* upsert */ false, context.asyncAssertSuccess(save -> {
      context.assertEquals(id, save);
      postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
        context.assertEquals("x", get.getString("key"));
        postgresClient.save(FOO, id, singleQuotePojo, /* returnId */ true, /* upsert */ false, context.asyncAssertFailure(update -> {
          context.assertTrue(update.getMessage().contains("duplicate key"), update.getMessage());
          postgresClient.getById(FOO, id, context.asyncAssertSuccess(get2 -> {
            context.assertEquals("x", get2.getString("key"));
          }));
        }));
      }));
    }));
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
  public void upsert4(TestContext context) {
    String id = randomUuid();
    PostgresClient postgresClient = createFoo(context);
    postgresClient.upsert(FOO, id, xPojo, context.asyncAssertSuccess(save -> {
      context.assertEquals(id, save);
      postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
        context.assertEquals("x", get.getString("key"));
        postgresClient.upsert(FOO, id, singleQuotePojo, context.asyncAssertSuccess(update -> {
          context.assertEquals(id, update);
          postgresClient.getById(FOO, id, context.asyncAssertSuccess(get2 -> {
            context.assertEquals("'", get2.getString("key"));
          }));
        }));
      }));
    }));
  }

  @Test
  public void upsert5(TestContext context) {
    String id = randomUuid();
    PostgresClient postgresClient = createFoo(context);
    postgresClient.upsert(FOO, id, xPojo, /* convertEntity */ true, context.asyncAssertSuccess(save -> {
      context.assertEquals(id, save);
      postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
        context.assertEquals("x", get.getString("key"));
        postgresClient.upsert(FOO, id, singleQuotePojo, /* convertEntity */ true, context.asyncAssertSuccess(update -> {
          context.assertEquals(id, update);
          postgresClient.getById(FOO, id, context.asyncAssertSuccess(get2 -> {
            context.assertEquals("'", get2.getString("key"));
          }));
        }));
      }));
    }));
  }

  private String base64(byte [] source) {
    return Base64.getEncoder().withoutPadding().encodeToString(source);
  }

  @Test
  public void upsert5binary(TestContext context) {
    String id = randomUuid();
    byte [] witloof = "witloof".getBytes();
    byte [] weld = "weld".getBytes();
    PostgresClient postgresClient = createFooBinary(context);
    postgresClient.upsert(FOO, id, new JsonArray().add(witloof), /* convertEntity */ false, context.asyncAssertSuccess(save -> {
      context.assertEquals(id, save);
      String fullTable = PostgresClient.convertToPsqlStandard(TENANT) + "." + FOO;
      postgresClient.select("SELECT jsonb FROM " + fullTable, context.asyncAssertSuccess(select -> {
        context.assertEquals(base64(witloof), select.iterator().next().getString("jsonb"), "select");
        postgresClient.upsert(FOO, id, new JsonArray().add(weld), /* convertEntity */ false, context.asyncAssertSuccess(update -> {
          context.assertEquals(id, update);
          postgresClient.select("SELECT jsonb FROM " + fullTable, context.asyncAssertSuccess(select2 -> {
            context.assertEquals(base64(weld), select2.iterator().next().getString("jsonb"), "select2");
          }));
        }));
      }));
    }));
  }

  @Test
  public void save7(TestContext context) {
    String id = randomUuid();
    PostgresClient postgresClient = createFoo(context);
    postgresClient.save(FOO, id, xPojo, true, true, true, context.asyncAssertSuccess(save -> {
      context.assertEquals(id, save);
      postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
        context.assertEquals("x", get.getString("key"));
        postgresClient.save(FOO, id, singleQuotePojo, true, true, true, context.asyncAssertSuccess(update -> {
          context.assertEquals(id, update);
          postgresClient.getById(FOO, id, context.asyncAssertSuccess(get2 -> {
            context.assertEquals("'", get2.getString("key"));
          }));
        }));
      }));
    }));
  }

  @Test
  public void save7binary(TestContext context) {
    String id = randomUuid();
    byte [] apple = "apple".getBytes();
    byte [] banana = "banana".getBytes();
    PostgresClient postgresClient = createFooBinary(context);
    postgresClient.save(FOO, id, new JsonArray().add(apple), true, true, false, context.asyncAssertSuccess(save -> {
      context.assertEquals(id, save);
      String fullTable = PostgresClient.convertToPsqlStandard(TENANT) + "." + FOO;
      postgresClient.select("SELECT jsonb FROM " + fullTable, context.asyncAssertSuccess(select -> {
        context.assertEquals(base64(apple), select.iterator().next().getString("jsonb"), "select");
        postgresClient.save(FOO, id, new JsonArray().add(banana), true, true, false, context.asyncAssertSuccess(update -> {
          context.assertEquals(id, update);
          postgresClient.select("SELECT jsonb FROM " + fullTable, context.asyncAssertSuccess(select2 -> {
            context.assertEquals(base64(banana), select2.iterator().next().getString("jsonb"), "select2");
          }));
        }));
      }));
    }));
  }

  @Test
  public void saveTrans4(TestContext context) {
    postgresClient = createFoo(context);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.save(trans, FOO, xPojo, context.asyncAssertSuccess(save -> {
        String id = save;
        postgresClient.endTx(trans, context.asyncAssertSuccess(end -> {
          postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
            context.assertEquals("x", get.getString("key"));
          }));
        }));
      }));
    }));
  }

  @Test
  public void saveTrans5(TestContext context) {
    String id = randomUuid();
    postgresClient = createFoo(context);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.save(trans, FOO, id, xPojo, context.asyncAssertSuccess(save -> {
        postgresClient.endTx(trans, context.asyncAssertSuccess(end -> {
          postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
            context.assertEquals("x", get.getString("key"));
          }));
        }));
      }));
    }));
  }

  @Test
  public void saveTrans7(TestContext context) {
    String id = randomUuid();
    postgresClient = createFoo(context);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.save(trans, FOO, id, xPojo, true, true, context.asyncAssertSuccess(save -> {
        postgresClient.endTx(trans, context.asyncAssertSuccess(end -> {
          postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
            context.assertEquals("x", get.getString("key"));
          }));
        }));
      }));
    }));
  }

  @Test
  public void saveBatch(TestContext context) {
    String id1 = randomUuid();
    List<StringPojo> list = new ArrayList<>();
    list.add(xPojo);
    list.add(new StringPojo("v", id1));
    postgresClient = createFoo(context);
    postgresClient.saveBatch(FOO, list, context.asyncAssertSuccess(save -> {
      String id0 = save.iterator().next().getValue(0).toString();
      postgresClient.getById(FOO, id0, context.asyncAssertSuccess(get -> {
        context.assertEquals("x", get.getString("key"));
      }));
      postgresClient.getById(FOO, id1, context.asyncAssertSuccess(get -> {
        context.assertEquals("v", get.getString("key"));
      }));
    }));
  }

  @Test
  public void updateBatch(TestContext context) {
    String id1 = randomUuid();
    String id2 = randomUuid();
    JsonArray a1 = new JsonArray()
        .add(new JsonObject().put("id", id1).put("a", 1).encode())
        .add(new JsonObject().put("id", id2).put("b", 2).encode());
    JsonArray a2 = new JsonArray()
        .add(new JsonObject().put("id", id1).put("a", 3).encode())
        .add(new JsonObject().put("id", id2).put("b", 4).encode());
    postgresClient = createFoo(context);
    postgresClient.updateBatch(FOO, a1, context.asyncAssertSuccess(update -> {
      assertThat(update.rowCount(), is(0));
      assertThat(update.next().rowCount(), is(0));
      postgresClient.saveBatch(FOO, a1, context.asyncAssertSuccess(save -> {
        assertThat(save.rowCount(), is(1));
        assertThat(save.next().rowCount(), is(1));
        postgresClient.updateBatch(FOO, a2, context.asyncAssertSuccess(update2 -> {
          assertThat(update2.rowCount(), is(1));
          assertThat(update2.next().rowCount(), is(1));
          assertThat(update2.next().next(), is(nullValue()));
          postgresClient.getById(FOO, id2, context.asyncAssertSuccess(get -> {
            assertThat(get.getInteger("b"), is(4));
          }));
        }));
      }));
    }));
  }

  @Test
  public void updateBatchPojo(TestContext context) {
    String id1 = randomUuid();
    String id2 = randomUuid();
    List<StringPojo> a1 = new ArrayList<>();
    a1.add(new StringPojo("a", id1));
    a1.add(new StringPojo("b", id2));
    List<StringPojo> a2 = new ArrayList<>();
    a2.add(new StringPojo("c", id1));
    a2.add(new StringPojo("d", id2));
    postgresClient = createFoo(context);
    postgresClient.updateBatch(FOO, a1, context.asyncAssertSuccess(update -> {
      assertThat(update.rowCount(), is(0));
      assertThat(update.next().rowCount(), is(0));
      postgresClient.saveBatch(FOO, a1, context.asyncAssertSuccess(save -> {
        assertThat(save.rowCount(), is(1));
        assertThat(save.next().rowCount(), is(1));
        postgresClient.updateBatch(FOO, a2, context.asyncAssertSuccess(update2 -> {
          assertThat(update2.rowCount(), is(1));
          assertThat(update2.next().rowCount(), is(1));
          assertThat(update2.next().next(), is(nullValue()));
          postgresClient.getById(FOO, id2, context.asyncAssertSuccess(get -> {
            assertThat(get.getString("key"), is("d"));
          }));
        }));
      }));
    }));
  }

  @Test
  public void upsertBatch(TestContext context) {
    String id1 = randomUuid();
    String id2 = randomUuid();
    String id3 = randomUuid();
    String id4 = randomUuid();
    List<StringPojo> a = new ArrayList<>();
    a.add(new StringPojo("a1", id1));
    a.add(new StringPojo("a2", id2));
    a.add(new StringPojo("a3", id3));
    List<StringPojo> b = new ArrayList<>();
    b.add(new StringPojo("b1", id1));
    b.add(new StringPojo("b3", id3));
    b.add(new StringPojo("b4", id4));
    b.add(new StringPojo("b5"));
    postgresClient = createFoo(context);
    postgresClient.saveBatch(FOO, a, context.asyncAssertSuccess(save -> {
      postgresClient.upsertBatch(FOO, b, context.asyncAssertSuccess(upsert -> {
        String id5 = upsert.next().next().next().iterator().next().getValue(0).toString();
        postgresClient.getById(FOO, id1, context.asyncAssertSuccess(get -> {
          context.assertEquals("b1", get.getString("key"));
        }));
        postgresClient.getById(FOO, id4, context.asyncAssertSuccess(get -> {
          context.assertEquals("b4", get.getString("key"));
        }));
        postgresClient.getById(FOO, id5, context.asyncAssertSuccess(get -> {
          context.assertEquals("b5", get.getString("key"));
        }));
      }));
    }));
  }

  @Test
  public void saveBatchXTrans(TestContext context) {
    List<Object> list = Collections.singletonList(xPojo);
    postgresClient = createFoo(context);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.saveBatch(trans, FOO, list, context.asyncAssertSuccess(save -> {
        final String id = save.iterator().next().getValue(0).toString();
        postgresClient.endTx(trans, context.asyncAssertSuccess(end -> {
          postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
            context.assertEquals("x", get.getString("key"));
          }));
        }));
      }));
    }));
  }

  @Test
  public void upsertBatchXTrans(TestContext context) {
    List<Object> list = Collections.singletonList(xPojo);
    postgresClient = createFoo(context);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.upsertBatch(trans, FOO, list, context.asyncAssertSuccess(save -> {
        final String id = save.iterator().next().getValue(0).toString();
        postgresClient.endTx(trans, context.asyncAssertSuccess(end -> {
          postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
            context.assertEquals("x", get.getString("key"));
          }));
        }));
      }));
    }));
  }


  @Test
  public void endTxTransFailed(TestContext context) {
    postgresClient = postgresClient(TENANT);
    Promise<SQLConnection> promise = Promise.promise();
    promise.fail("failure");
    AsyncResult<SQLConnection> trans = promise.future();

    postgresClient.endTx(trans, context.asyncAssertFailure(res ->
        context.assertEquals("failure", res.getMessage())));
  }

  @Test
  public void rollbackTransFailed(TestContext context) {
    postgresClient = postgresClient(TENANT);
    Promise<SQLConnection> promise = Promise.promise();
    promise.fail("failure");
    AsyncResult<SQLConnection> trans = promise.future();

    postgresClient.rollbackTx(trans, context.asyncAssertFailure(res ->
        context.assertEquals("failure", res.getMessage())));
  }


  @Test
  public void endTxNullConnection(TestContext context) {
    postgresClient = createFoo(context);
    postgresClient.startTx(context.asyncAssertSuccess(trans1 -> {
      Promise<SQLConnection> trans2 = Promise.promise();
      SQLConnection conn = new SQLConnection(null, trans1.tx, null);
      trans2.complete(conn);
      postgresClient.endTx(trans2.future(), context.asyncAssertSuccess());
    }));
  }

  @Test
  public void endTxNullTransaction(TestContext context) {
    postgresClient = createFoo(context);
    postgresClient.startTx(context.asyncAssertSuccess(trans1 -> {
      Promise<SQLConnection> trans2 = Promise.promise();
      SQLConnection conn = new SQLConnection(trans1.conn, null, null);
      trans2.complete(conn);
      postgresClient.endTx(trans2.future(), context.asyncAssertFailure());
    }));
  }

  @Test
  public void rollbackTxNullTransaction(TestContext context) {
    postgresClient = createFoo(context);
    postgresClient.startTx(context.asyncAssertSuccess(trans1 -> {
      Promise<SQLConnection> trans2 = Promise.promise();
      SQLConnection conn = new SQLConnection(trans1.conn, null, null);
      trans2.complete(conn);
      postgresClient.rollbackTx(trans2.future(), context.asyncAssertFailure());
    }));
  }

  @Test
  public void endTxNormal(TestContext context) {
    Async async = context.async();
    postgresClient = createFoo(context);
    postgresClient.startTx(trans -> {
      context.assertTrue(trans.succeeded());
      postgresClient.endTx(trans, context.asyncAssertSuccess(x -> async.complete()));
    });
  }

  @Test
  public void testFinalizeTx(TestContext context) {
    Promise<Void> promise = Promise.promise();
    promise.fail("transaction error");
    PostgresClient.finalizeTx(promise.future(), null,
        context.asyncAssertFailure(x -> context.assertEquals("transaction error", x.getMessage())));
  }

  @Test
  public void saveBatchXTrans2(TestContext context) {
    List<Object> list = new LinkedList<>();
    list.add(context);
    postgresClient = createFoo(context);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.saveBatch(trans, FOO, list, context.asyncAssertFailure());
      // the failure automatically rolls back the transaction
    }));
  }

  @Test
  public void saveBatchNullConnection(TestContext context) {
    List<Object> list = Collections.singletonList(xPojo);
    postgresClientNullConnection().saveBatch(FOO, list, context.asyncAssertFailure());
  }

  @Test
  public void saveBatchNullList(TestContext context) {
    createFoo(context).saveBatch(BAR, (List<Object>)null, context.asyncAssertSuccess(save -> {
      context.assertEquals(0, save.size());
    }));
  }

  @Test
  public void saveBatchEmptyList(TestContext context) {
    List<Object> list = Collections.emptyList();
    createFoo(context).saveBatch(FOO, list, context.asyncAssertSuccess(save -> {
      context.assertEquals(0, save.size());
    }));
  }

  @Test
  public void saveBatchNullEntity(TestContext context) {
    List<Object> list = new ArrayList<>();
    list.add(null);
    createFoo(context).saveBatch(FOO, list, context.asyncAssertFailure());
  }

  @Test
  public void saveBatchGetConnectionFails(TestContext context) {
    List<Object> list = Collections.singletonList(xPojo);
    postgresClientGetConnectionFails().saveBatch(FOO, list, context.asyncAssertFailure());
  }

  @Test
  public void saveBatchJson(TestContext context) {
    String id = randomUuid();
    JsonArray array = new JsonArray()
        .add("{ \"x\" : \"a\" }")
        .add("{ \"y\" : \"z\", \"id\": \"" + id + "\" }")
        .add("{ \"z\" : \"'\" }");
    createFoo(context).saveBatch(FOO, array, context.asyncAssertSuccess(res -> {
      // iterate over all RowSets in batch result to get total count
      RowSet<Row> resCurrent = res;
      int total = 0;
      while (resCurrent != null) {
        total += resCurrent.size();
        resCurrent = resCurrent.next();
      }
      context.assertEquals(3, total);
      context.assertEquals("id", res.columnsNames().get(0));

      // second set
      Row row = res.next().iterator().next();
      context.assertEquals(id, row.getValue("id").toString());
      postgresClient.getById(FOO, id, context.asyncAssertSuccess(get -> {
        context.assertEquals("z", get.getString("y"));
      }));
    }));
  }

  @Test
  public void upsertBatchJson(TestContext context) {
    String id1 = randomUuid();
    String id2 = randomUuid();
    String id3 = randomUuid();
    String id4 = randomUuid();
    List<StringPojo> a = new ArrayList<>();
    a.add(new StringPojo("a1", id1));
    a.add(new StringPojo("a2", id2));
    a.add(new StringPojo("a3", id3));
    JsonArray b = new JsonArray()
        .add("{ \"key\" : \"b1\", \"id\": \"" + id1 + "\" }")
        .add("{ \"key\" : \"b3\", \"id\": \"" + id3 + "\" }")
        .add("{ \"key\" : \"b4\", \"id\": \"" + id4 + "\" }")
        .add("{ \"key\" : \"b5\"                          }");
    postgresClient = createFoo(context);
    postgresClient.saveBatch(FOO, a, context.asyncAssertSuccess(save -> {
      postgresClient.upsertBatch(FOO, b, context.asyncAssertSuccess(upsert -> {
        String id5 = upsert.next().next().next().iterator().next().getValue(0).toString();
        postgresClient.getById(FOO, id1, context.asyncAssertSuccess(get -> {
          context.assertEquals("b1", get.getString("key"));
        }));
        postgresClient.getById(FOO, id4, context.asyncAssertSuccess(get -> {
          context.assertEquals("b4", get.getString("key"));
        }));
        postgresClient.getById(FOO, id5, context.asyncAssertSuccess(get -> {
          context.assertEquals("b5", get.getString("key"));
        }));
      }));
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
  public void saveBatchJsonNullArray(TestContext context) {
    createFoo(context).saveBatch(FOO, (JsonArray)null, context.asyncAssertSuccess(save -> {
      context.assertEquals(0, save.size());
    }));
  }

  @Test
  public void saveBatchJsonEmptyArray(TestContext context) {
    createFoo(context).saveBatch(FOO, new JsonArray(), context.asyncAssertSuccess(save -> {
      context.assertEquals(0, save.size());
    }));
  }

  @Test
  public void saveBatchJsonNullEntity(TestContext context) {
    JsonArray array = new JsonArray();
    array.add((String) null);
    createFoo(context).saveBatch(FOO, array, context.asyncAssertFailure());
  }

  @Test
  public void saveBatchJsonFailedConnection(TestContext context) {
    postgresClient().saveBatch(Future.failedFuture("f"), FOO, new JsonArray(),
        context.asyncAssertFailure(e -> assertThat(e.getMessage(), is("f"))));
  }

  @Test
  public void saveBatchJsonConnection(TestContext context) {
    String id = randomUuid();
    JsonArray update = new JsonArray().add(new JsonObject().put("id", id).put("key", "y").encode());
    createFoo(context).save(FOO, id, new StringPojo("x"), context.asyncAssertSuccess(x -> {
      postgresClient.getSQLConnection(sqlConnection -> {
        postgresClient.saveBatch(sqlConnection, FOO, update, context.asyncAssertFailure(e -> {
          postgresClient.upsertBatch(sqlConnection, FOO, update, context.asyncAssertSuccess(rowSet -> {
            postgresClient.getById(FOO, id, context.asyncAssertSuccess(json -> {
              assertThat(rowSet.iterator().next().getUUID(0).toString(), is(id));
              assertThat(json.getString("key"), is("y"));
            }));
          }));
        }));
      });
    }));
  }

  @Test
  public void upsertBatchJsonConnectionException(TestContext context) {
    postgresClient().upsertBatch(Future.succeededFuture(), FOO, new JsonArray(),
        context.asyncAssertFailure(e -> assertThat(e, is(instanceOf(NullPointerException.class)))));
  }

  @Test
  public void saveTrans(TestContext context) {
    postgresClient = createFoo(context);
    String uuid = randomUuid();
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.save(trans, FOO,uuid, xPojo, context.asyncAssertSuccess(id -> {
        Criterion filter = new Criterion(new Criteria().addField("id").setJSONB(false)
            .setOperation("=").setVal(id));
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
        Criterion filter = new Criterion(new Criteria().addField("id").setJSONB(false)
            .setOperation("=").setVal(id));
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
    AsyncResult<SQLConnection> conn = null;
    postgresClient.save(conn, FOO, xPojo, context.asyncAssertFailure());
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
  public void saveAndReturnUpdatedEntity(TestContext context) {
    postgresClient = createFoo(context);
    String uuid1 = randomUuid();
    postgresClient.saveAndReturnUpdatedEntity(FOO, uuid1, xPojo, context.asyncAssertSuccess(updated -> {
      context.assertEquals("x", updated.key);
      postgresClient.getById(FOO, uuid1, context.asyncAssertSuccess(get -> {
        context.assertEquals("x", get.getString("key"));
      }));
    }));
    String uuid2 = randomUuid();
    postgresClient.saveAndReturnUpdatedEntity(FOO, uuid2, singleQuotePojo, context.asyncAssertSuccess(updated -> {
      context.assertEquals("'", updated.key);
      postgresClient.getById(FOO, uuid2, context.asyncAssertSuccess(get -> {
        context.assertEquals("'", get.getString("key"));
      }));
    }));
  }

  @Test
  public void saveAndReturnUpdatedEntityWithNullId(TestContext context) {
    createFoo(context).saveAndReturnUpdatedEntity(FOO, null, xPojo, context.asyncAssertSuccess(updated -> {
      context.assertEquals("x", updated.key);
    }));
  }

  @Test
  public void saveAndReturnUpdatedEntityNullConnection(TestContext context) {
    String uuid = randomUuid();
    postgresClientNullConnection().saveAndReturnUpdatedEntity(FOO, uuid, xPojo, context.asyncAssertFailure());
  }

  @Test
  public void saveAndReturnUpdatedEntityGetConnectionFails(TestContext context) {
    String uuid = randomUuid();
    postgresClientGetConnectionFails().saveAndReturnUpdatedEntity(FOO, uuid, xPojo, context.asyncAssertFailure());
  }

  @Test
  public void saveAndReturnUpdatedEntityQueryFails(TestContext context) {
    String uuid = randomUuid();
    postgresClientQueryFails().saveAndReturnUpdatedEntity(FOO, uuid, xPojo, context.asyncAssertFailure());
  }

  @Test
  public void saveAndReturnUpdatedEntityQueryReturnBadResults(TestContext context) {
    String uuid = randomUuid();
    postgresClientQueryReturnBadResults().saveAndReturnUpdatedEntity(FOO, uuid, xPojo, context.asyncAssertFailure());
  }

  @Test
  public void saveAndReturnUpdatedEntityCreatingPojoFails(TestContext context) {
    class FailingPojo extends StringPojo {
      public FailingPojo() {  // this constructor is called when deserialising the JSON returned from DB
        throw new RuntimeException();
      }
      public FailingPojo(String key, String id) {
        this.key = key;
        this.id = id;
      }
    };
    String uuid = randomUuid();
    FailingPojo failingPojo = new FailingPojo("y", uuid);
    postgresClient = createFoo(context);
    postgresClient.saveAndReturnUpdatedEntity(FOO, uuid, failingPojo, context.asyncAssertFailure(e ->
      assertThat(e, is(instanceOf(UncheckedIOException.class)))));
  }

  @Test
  public void saveTransIdNull(TestContext context) {
    String id = randomUuid();
    postgresClient = createFoo(context);
    AsyncResult<SQLConnection> conn = null;
    postgresClient.save(conn, FOO, id, xPojo, context.asyncAssertFailure());
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
      context.assertEquals(0, res.result().size());
      context.assertEquals("id", res.result().columnsNames().get(0));
      async.complete();
    });
  }

  @Test
  public void saveBatchSingleQuote(TestContext context) {
    List<Object> list = Collections.singletonList(singleQuotePojo);
    createFoo(context).saveBatch(FOO, list, context.asyncAssertSuccess(res -> {
      context.assertEquals(1, res.size());
    }));
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
      String sql = "WHERE id='" + id + "'";
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
  public void getByIdAsStringForUpdate(TestContext context) {
    Async async1 = context.async();
    Async async2 = context.async();
    String id = randomUuid();
    postgresClient = createFoo(context);
    postgresClient.save(FOO, id, new StringPojo("x0"), res -> {
      assertSuccess(context, res);
      context.assertEquals(id, res.result());
      Criterion criterion = new Criterion();
      criterion.addCriterion(new Criteria().addField("'id'").setOperation("=").setVal(id));
      postgresClient.startTx(conn -> {
        // lock the row for update
        postgresClient.getByIdAsStringForUpdate(conn, FOO, id, get -> {
          // concurrent update will have to wait
          postgresClient.update(FOO, new StringPojo("x2"), id, rows -> {
            context.assertTrue(async1.isCompleted());
            postgresClient.getByIdAsString(FOO, id, get2 -> {
              assertSuccess(context, get2);
              context.assertTrue(get2.result().contains("\"x2\""));
              async2.complete();
            });
          });
          // update first because it holds the lock
          postgresClient.execute(conn, "SELECT pg_sleep(0.5)", delayRs -> {
            assertSuccess(context, delayRs);
            postgresClient.update(conn, FOO, new StringPojo("x1"),
                new CQLWrapper(criterion), true, rows -> {
              assertSuccess(context, rows);
              context.assertFalse(async2.isCompleted());
              async1.complete();
              // before the first update is committed
              postgresClient.getByIdAsString(FOO, id, get0 -> {
                assertSuccess(context, get0);
                context.assertTrue(get0.result().contains("\"x0\""), get0.result());
                // read in the same update transaction
                postgresClient.getByIdAsString(conn, FOO, id, get1 -> {
                  assertSuccess(context, get1);
                  context.assertTrue(get1.result().contains("\"x1\""), get1.result());
                  postgresClient.endTx(conn, context.asyncAssertSuccess());
                });
              });
            });
          });
        });
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
  public void getByCriterion(TestContext context) {
    JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
    PostgresClient postgresClient = insertXAndSingleQuotePojo(context, ids);
    Criterion criterion = new Criterion();
    criterion.addCriterion(new Criteria().addField("'key'").setOperation("=").setVal("x"));
    postgresClient.get(FOO, StringPojo.class, criterion)
    .onComplete(context.asyncAssertSuccess(res -> {
      assertThat(res.getResults().size(), is(1));
      assertThat(res.getResults().get(0).getId(), is(ids.getString(0)));
    }));
  }

  @Test
  public void getByCriterion2(TestContext context) {
    JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
    PostgresClient postgresClient = insertXAndSingleQuotePojo(context, ids);
    Criterion criterion = new Criterion();
    criterion.addCriterion(new Criteria().addField("'key'").setOperation("=").setVal("x"));
    postgresClient.get(FOO, StringPojo.class, criterion, false, context.asyncAssertSuccess(res -> {
      assertThat(res.getResults().size(), is(1));
      assertThat(res.getResults().get(0).getId(), is(ids.getString(0)));
    }));
  }

  @Test
  public void getByCriterionFacets(TestContext context) {
    JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
    PostgresClient postgresClient = insertXAndSingleQuotePojo(context, ids);
    Criterion criterion = new Criterion();
    criterion.addCriterion(new Criteria().addField("'key'").setOperation("=").setVal("x"));
    postgresClient.get(FOO, StringPojo.class, criterion, false, false, (List<FacetField>)null)
    .onComplete(context.asyncAssertSuccess(res -> {
      assertThat(res.getResults().size(), is(1));
      assertThat(res.getResults().get(0).getId(), is(ids.getString(0)));
    }));
  }

  @Test
  public void getByCriterionWithIdColumn(TestContext context) {
    JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
    PostgresClient postgresClient = insertXAndSingleQuotePojo(context, ids);
    Criterion criterion = new Criterion();
    criterion.addCriterion(new Criteria().addField("id").setJSONB(false).setOperation("=").setVal(ids.getString(0)));
    postgresClient.get(FOO, StringPojo.class, criterion, false, context.asyncAssertSuccess(res -> {
      assertThat(res.getResults().size(), is(1));
      assertThat(res.getResults().get(0).key, is("x"));
    }));
  }

  // broken since this RMB-497 commit:
  // https://github.com/folio-org/raml-module-builder/commit/51a67d3b81b372096c11ddcc8e7b0af6db48c744
  @Ignore("broken since RMB-497, see RMB-817")
  @Test
  public void getByExamplePojo(TestContext context) {
    PostgresClient postgresClient = insertXAndSingleQuotePojo(context, new JsonArray().add(randomUuid()).add(randomUuid()));
    postgresClient.get(FOO, new StringPojo("x"), false, context.asyncAssertSuccess(res -> {
      assertThat(res.getResults().size(), is(1));
      assertThat(res.getResults().get(0).key, is("x"));
    }));
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
    PgPool client = new PgPoolBase();
    try {
      PostgresClient postgresClient = new PostgresClient(vertx, TENANT);
      postgresClient.setClient(client);
      return postgresClient;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return a PostgresClient where getConnection(handler) invokes the handler with a failure.
   */
  private PostgresClient postgresClientGetConnectionFails() {
    PgPool client = new PgPoolBase() {
      @Override
      public Future<SqlConnection> getConnection() {
        return Future.failedFuture("postgresClientGetConnectionFails");
      }
    };
    try {
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
    PgConnection pgConnection = new PgConnection() {
      @Override
      public PgConnection notificationHandler(Handler<PgNotification> handler) {
        throw new RuntimeException();
      }

      @Override
      public PgConnection cancelRequest(Handler<AsyncResult<Void>> handler) {
        throw new RuntimeException();
      }

      @Override
      public int processId() {
        throw new RuntimeException();
      }

      @Override
      public int secretKey() {
        throw new RuntimeException();
      }

      @Override
      public PgConnection prepare(String s, Handler<AsyncResult<PreparedStatement>> handler) {
        throw new RuntimeException();
      }

      @Override
      public Future<PreparedStatement> prepare(String s) {
        return null;
      }

      @Override
      public PgConnection exceptionHandler(Handler<Throwable> handler) {
        return null;
      }

      @Override
      public PgConnection closeHandler(Handler<Void> handler) {
        return null;
      }

      @Override
      public void begin(
          Handler<AsyncResult<Transaction>> handler) {

      }

      @Override
      public Future<Transaction> begin() {
        return null;
      }

      @Override
      public boolean isSSL() {
        return false;
      }

      @Override
      public void close(Handler<AsyncResult<Void>> handler) {
        close().onComplete(handler);
      }

      @Override
      public Future<Void> close() {
        return Future.succeededFuture();
      }

      @Override
      public Query<RowSet<Row>> query(String s) {
        throw new RuntimeException();
      }

      @Override
      public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
        throw new RuntimeException();
      }

      @Override
      public DatabaseMetadata databaseMetadata() {
        throw new RuntimeException();
      }
    };

    PgPool client = new PgPoolBase() {
      @Override
      public Future<SqlConnection> getConnection() {
        return Future.succeededFuture(pgConnection);
      }
    };
    try {
      PostgresClient postgresClient = new PostgresClient(vertx, TENANT);
      postgresClient.setClient(client);
      return postgresClient;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return a PostgresClient where invoking SQLConnection::update, SQLConnection::updateWithParams or
   * SQLConnection::queryWithParams will report a failure via the resultHandler.
   */
  private PostgresClient postgresClientQueryFails() {
    PgConnection pgConnection = new PgConnection() {
      @Override
      public PgConnection notificationHandler(Handler<PgNotification> handler) {
        return this;
      }

      @Override
      public PgConnection cancelRequest(Handler<AsyncResult<Void>> handler) {
        handler.handle(Future.failedFuture("cancelRequestFails"));
        return this;
      }

      @Override
      public int processId() {
        return 0;
      }

      @Override
      public int secretKey() {
        return 0;
      }

      @Override
      public PgConnection prepare(String s, Handler<AsyncResult<PreparedStatement>> handler) {
        prepare(s).onComplete(handler);
        return null;
      }

      @Override
      public Future<PreparedStatement> prepare(String s) {
        return Future.failedFuture("preparedFails");
      }

      @Override
      public PgConnection exceptionHandler(Handler<Throwable> handler) {
        return null;
      }

      @Override
      public PgConnection closeHandler(Handler<Void> handler) {
        return null;
      }

      @Override
      public void begin(
          Handler<AsyncResult<Transaction>> handler) {

      }

      @Override
      public Future<Transaction> begin() {
        return null;
      }

      @Override
      public boolean isSSL() {
        return false;
      }

      @Override
      public void close(Handler<AsyncResult<Void>> handler) {

      }

      @Override
      public Query<RowSet<Row>> query(String s)
      {
        return new Query<RowSet<Row>> () {

          @Override
          public void execute(Handler<AsyncResult<RowSet<Row>>> handler) {
            handler.handle(execute());
          }

          @Override
          public Future<RowSet<Row>> execute() {
            return Future.failedFuture("queryFails");
          }

          @Override
          public <R> Query<SqlResult<R>> collecting(Collector<Row, ?, R> collector) {
            return null;
          }

          @Override
          public <U> Query<RowSet<U>> mapping(Function<Row, U> function) {
            return null;
          }
        };
      }

      @Override
      public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
        throw new RuntimeException("queryFails");
      }

      @Override
      public Future<Void> close() {
        return null;
      }

      @Override
      public DatabaseMetadata databaseMetadata() {
        return null;
      }
    };

    PgPool client = new PgPoolBase() {
      @Override
      public Future<SqlConnection> getConnection() {
        return Future.succeededFuture(pgConnection);
      }
    };
    try {
      PostgresClient postgresClient = new PostgresClient(vertx, TENANT);
      postgresClient.setClient(client);
      return postgresClient;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return a PostgresClient where invoking SQLConnection::queryWithParams will return null ResultSet
   */
  private PostgresClient postgresClientQueryReturnBadResults() {
    PgConnection pgConnection = new PgConnection() {
      @Override
      public PgConnection notificationHandler(Handler<PgNotification> handler) {
        return null;
      }

      @Override
      public PgConnection cancelRequest(Handler<AsyncResult<Void>> handler) {
        return this;
      }

      @Override
      public int processId() {
        return 0;
      }

      @Override
      public int secretKey() {
        return 0;
      }

      @Override
      public PgConnection prepare(String s, Handler<AsyncResult<PreparedStatement>> handler) {
        return null;
      }

      @Override
      public Future<PreparedStatement> prepare(String s) {
        return null;
      }

      @Override
      public PgConnection exceptionHandler(Handler<Throwable> handler) {
        return null;
      }

      @Override
      public PgConnection closeHandler(Handler<Void> handler) {
        return null;
      }

      @Override
      public void begin(
          Handler<AsyncResult<Transaction>> handler) {

      }

      @Override
      public Future<Transaction> begin() {
        return null;
      }

      @Override
      public boolean isSSL() {
        return false;
      }

      @Override
      public void close(Handler<AsyncResult<Void>> handler) {

      }

      @Override
      public Query<RowSet<Row>> query(String s) {
        return null;
      }

      @Override
      public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
        return null;
      }

      @Override
      public Future<Void> close() {

        return null;
      }

      @Override
      public DatabaseMetadata databaseMetadata() {
        return null;
      }
    };
    PgPool client = new PgPoolBase() {
      @Override
      public Future<SqlConnection> getConnection() {
        return Future.succeededFuture(pgConnection);
      }
    };
    try {
      PostgresClient postgresClient = new PostgresClient(vertx, TENANT);
      postgresClient.setClient(client);
      return postgresClient;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void executeOK(TestContext context) {
    JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
    insertXAndSingleQuotePojo(context, ids)
        .execute("DELETE FROM tenant_raml_module_builder.foo WHERE id='" + ids.getString(1) + "'",
            context.asyncAssertSuccess(res -> {
              context.assertEquals(1, res.rowCount());
            }));
  }

  @Test
  public void executeOKFuture(TestContext context) {
    JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
    insertXAndSingleQuotePojo(context, ids)
        .execute("DELETE FROM tenant_raml_module_builder.foo WHERE id='" + ids.getString(1) + "'")
        .onComplete(context.asyncAssertSuccess(res -> {
          context.assertEquals(1, res.rowCount());
        }));
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
    JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
    insertXAndSingleQuotePojo(context, ids)
        .execute("DELETE FROM tenant_raml_module_builder.foo WHERE id=$1", Tuple.of(UUID.fromString(ids.getString(0))),
            context.asyncAssertSuccess(res -> context.assertEquals(1, res.rowCount())));
  }

  @Test
  public void executeParamFuture(TestContext context) {
    JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
    insertXAndSingleQuotePojo(context, ids)
        .execute("DELETE FROM tenant_raml_module_builder.foo WHERE id=$1", Tuple.of(UUID.fromString(ids.getString(0))))
        .onComplete(context.asyncAssertSuccess(res -> context.assertEquals(1, res.rowCount())));
  }

  @Test
  public void executeParamSyntaxError(TestContext context) {
    postgresClient().execute("'", Tuple.tuple(), context.asyncAssertFailure());
  }

  @Test
  public void executeParamGetConnectionFails(TestContext context) throws Exception {
    postgresClientGetConnectionFails().execute("SELECT 1", Tuple.tuple(), context.asyncAssertFailure());
  }

  @Test
  public void executeParamNullConnection(TestContext context) throws Exception {
    postgresClientNullConnection().execute("SELECT 1", Tuple.tuple(), context.asyncAssertFailure());
  }

  @Test
  public void executeParamConnectionException(TestContext context) throws Exception {
    postgresClientConnectionThrowsException().execute("SELECT 1", Tuple.tuple(), context.asyncAssertFailure());
  }

  @Test
  public void executeTrans(TestContext context) {
    Async async1 = context.async();
    JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
    PostgresClient postgresClient = insertXAndSingleQuotePojo(context, ids);
    postgresClient.startTx(trans -> {
      assertSuccess(context, trans);
      postgresClient.execute(trans, "DELETE FROM tenant_raml_module_builder.foo WHERE id='" + ids.getString(1) + "'", res -> {
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
      postgresClient.execute(trans, "DELETE FROM tenant_raml_module_builder.foo WHERE id='" + ids.getString(0) + "'", res -> {
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
    postgresClient = createFoo(context);
    postgresClient.startTx(trans -> {
      assertSuccess(context, trans);
      postgresClient.execute(trans, "'",
          context.asyncAssertFailure(exec -> {
                context.assertTrue(exec.getMessage().contains("unterminated quoted string"));
                postgresClient.rollbackTx(trans, context.asyncAssertSuccess(e -> async.complete()));
              }
          ));
    });
  }

  @Test
  public void executeTransNullConnection(TestContext context) throws Exception {
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
      postgresClient.execute(trans, "DELETE FROM tenant_raml_module_builder.foo WHERE id=$1",
          Tuple.of(UUID.fromString(ids.getString(1))), res -> {
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
      postgresClient.execute(trans, "DELETE FROM tenant_raml_module_builder.foo WHERE id=$1",
          Tuple.of(UUID.fromString(ids.getString(0))), res -> {
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
    postgresClient = createFoo(context);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.execute(trans, "'", Tuple.tuple(), context.asyncAssertFailure(execute -> {
        postgresClient.rollbackTx(trans, context.asyncAssertSuccess());
      }));
    }));
  }

  @Test
  public void executeTransParamNullConnection(TestContext context) throws Exception {
    Async async = context.async();
    postgresClient = postgresClient();
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.execute(null, "SELECT 1", Tuple.tuple(), context.asyncAssertFailure(execute -> {
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
    List<Tuple> list = new ArrayList<>(2);
    list.add(Tuple.of(UUID.fromString(ids.getString(0))));
    list.add(Tuple.of(UUID.fromString(ids.getString(1))));
    insertXAndSingleQuotePojo(context, ids).execute("DELETE FROM tenant_raml_module_builder.foo WHERE id=$1", list, res -> {
      assertSuccess(context, res);
      List<RowSet<Row>> result = res.result();
      context.assertEquals(2, result.size());
      context.assertEquals(1, result.get(0).rowCount());
      context.assertEquals(1, result.get(1).rowCount());
      async.complete();
    });
  }

  private void assertNRowSets(RowSet<Row> rowSet, int n) {
    int actual = 0;
    while (rowSet != null) {
      RowIterator<Row> iterator = rowSet.iterator();
      assertThat(iterator.next().getString(0), is("x"));
      assertThat(iterator.hasNext(), is(false));
      rowSet = rowSet.next();
      actual++;
    }
    assertThat(actual, is(n));
  }

  @Test
  public void executeList0EmptyTuples(TestContext context) {
    postgresClient().execute("SELECT 'x'", Arrays.asList())
    .onComplete(context.asyncAssertSuccess(res -> assertNRowSets(res, 0)));
  }

  @Test
  public void executeList1EmptyTuple(TestContext context) {
    postgresClient().execute("SELECT 'x'", Arrays.asList(Tuple.tuple()))
    .onComplete(context.asyncAssertSuccess(res -> assertNRowSets(res, 1)));
  }

  @Test
  public void executeList2EmptyTuples(TestContext context) {
    postgresClient().execute("SELECT 'x'", Arrays.asList(Tuple.tuple(), Tuple.tuple()))
    .onComplete(context.asyncAssertSuccess(res -> assertNRowSets(res, 2)));
  }

  private void assertNRowSets(List<RowSet<Row>> list, int n) {
    assertThat(list.size(), is(n));
    list.forEach(rowSet -> {
      RowIterator<Row> iterator = rowSet.iterator();
      assertThat(iterator.next().getString(0), is("y"));
      assertThat(iterator.hasNext(), is(false));
    });
  }

  @Test
  public void executeList0EmptyTuplesHandler(TestContext context) {
    postgresClient().execute("SELECT 'y'", Arrays.asList(), context.asyncAssertSuccess(
        res -> assertNRowSets(res, 0)));
  }

  @Test
  public void executeList1EmptyTupleHandler(TestContext context) {
    postgresClient().execute("SELECT 'y'", Arrays.asList(Tuple.tuple()), context.asyncAssertSuccess(
        res -> assertNRowSets(res, 1)));
  }

  @Test
  public void executeList2EmptyTuplesHandler(TestContext context) {
    postgresClient().execute("SELECT 'y'", Arrays.asList(Tuple.tuple(), Tuple.tuple()), context.asyncAssertSuccess(
        res -> assertNRowSets(res, 2)));
  }

  @Test
  public void executeListNullParams(TestContext context) {
    postgresClient().execute("SELECT $1", (List<Tuple>) null)
    .onComplete(context.asyncAssertFailure(t -> assertThat(t, is(instanceOf(NullPointerException.class)))));
  }

  /** @return List containing one empty Tuple */
  private List<Tuple> list1JsonArray() {
    return Collections.singletonList(Tuple.tuple());
  }

  @Test
  public void executeListNullConnection(TestContext context) {
    postgresClientNullConnection().execute("SELECT 1", list1JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void executeListConnectionThrowsException(TestContext context) throws Exception {
    postgresClientConnectionThrowsException().execute("SELECT 1", list1JsonArray(), context.asyncAssertFailure());
  }

  @Test
  public void executeListFailure(TestContext context) {
    postgresClient().execute("SELECT foobar", Arrays.asList(Tuple.of("a")))
    .onComplete(context.asyncAssertFailure(t -> assertThat(t.getMessage(), containsString("foobar"))));
  }

  @Test
  public void executeListTrans(TestContext context) throws Exception {
    postgresClient().getSQLConnection(context.asyncAssertSuccess(conn -> {
      AsyncResult<SQLConnection> sqlConnection = Future.succeededFuture(conn);
      postgresClient.execute(sqlConnection, "SELECT 5", list1JsonArray(), context.asyncAssertSuccess(r -> {
        assertThat(r.size(), is(1));
        assertThat(r.get(0).iterator().next().getInteger(0), is(5));
      }));
    }));
  }

  @Test
  public void executeListConnectionFails(TestContext context) throws Exception {
    postgresClient().execute(Future.failedFuture("failed"), "SELECT 1", list1JsonArray(), context.asyncAssertFailure());
  }

  /** see {@link RunSQLIT} for more tests */
  @Test
  public void runSQLNull(TestContext context) throws Exception {
    postgresClient().runSQLFile(null, false).onComplete(context.asyncAssertFailure());
  }

  private PostgresClient createNumbers(TestContext context, int ...numbers) {
    PostgresClient postgresClient = createTable(context, TENANT, "numbers", "i INT");
    String schema = PostgresClient.convertToPsqlStandard(TENANT);
    StringBuilder s = new StringBuilder();
    for (int n : numbers) {
      if (s.length() > 0) {
        s.append(',');
      }
      s.append('(').append(n).append(')');
    }
    execute(context, "INSERT INTO " + schema + ".numbers VALUES " + s + ";");
    return postgresClient;
  }

  private String intsAsString(RowSet<Row> resultSet) {
    StringBuilder s = new StringBuilder();
    RowIterator<Row> iterator = resultSet.iterator();
    while (iterator.hasNext()) {
      if (s.length() > 0) {
        s.append(", ");
      }
      s.append(iterator.next().getInteger(0));
    }
    return s.toString();
  }

  private void intsAsString(RowStream<Row> sqlRowStream, Handler<AsyncResult<String>> replyHandler) {
    StringBuilder s = new StringBuilder();
    sqlRowStream.handler(row -> {
      if (s.length() > 0) {
        s.append(", ");
      }
      s.append(row.getInteger(0));
    }).exceptionHandler(e -> {
      replyHandler.handle(Future.failedFuture(e));
    }).endHandler(end -> {
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
    .select("SELECT i FROM numbers WHERE i IN ($1, $2, $3) ORDER BY i",
        Tuple.of(7, 9, 11), context.asyncAssertSuccess(select -> {
          context.assertEquals("7, 9",  intsAsString(select));
        }));
  }

  @Test
  public void selectParamTrans(TestContext context) {
    postgresClient = createNumbers(context, 11, 12, 13);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.select(trans, "SELECT i FROM numbers WHERE i IN ($1, $2, $3) ORDER BY i",
          Tuple.of(11, 13, 15), context.asyncAssertSuccess(select -> {
            postgresClient.endTx(trans, context.asyncAssertSuccess());
            context.assertEquals("11, 13",  intsAsString(select));
          }));
    }));
  }

  @Test
  public void selectStream(TestContext context) {
    List<Integer> list = new ArrayList<>();
    createNumbers(context, 21, 22, 23)
    .selectStream("SELECT i FROM numbers WHERE i IN (21, 23, 25) ORDER BY i", Tuple.tuple(),
        rowStream -> rowStream.handler(row -> list.add(row.getInteger(0))))
    .onComplete(context.asyncAssertSuccess(x -> assertThat(list.toString(), is("[21, 23]"))));
  }

  @Test
  public void selectStreamChunkSize(TestContext context) {
    List<Integer> list = new ArrayList<>();
    createNumbers(context, 21, 22, 23)
    .selectStream("SELECT i FROM numbers WHERE i IN (21, 23, 25) ORDER BY i", Tuple.tuple(), 2,
        rowStream -> rowStream.handler(row -> list.add(row.getInteger(0))))
    .onComplete(context.asyncAssertSuccess(x -> assertThat(list.toString(), is("[21, 23]"))));
  }

  @Test
  public void selectStreamTwoConnQueries(TestContext context) {
    List<Integer> list = new ArrayList<>();
    postgresClient = createNumbers(context, 21, 22, 23, 31, 32, 33);
    postgresClient.withTrans(
        conn -> conn.selectStream("SELECT i FROM numbers WHERE i IN (21, 23, 25) ORDER BY i", Tuple.tuple(),
            rowStream -> rowStream.handler(row -> list.add(row.getInteger(0))))
        .compose(x -> conn.selectStream("SELECT i FROM numbers WHERE i IN (31, 33, 35) ORDER BY i", Tuple.tuple(),
            rowStream -> rowStream.handler(row -> list.add(row.getInteger(0))))))
    .onComplete(context.asyncAssertSuccess(x -> assertThat(list.toString(), is("[21, 23, 31, 33]"))));
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
  public void selectStreamTransChunkSize(TestContext context) {
    postgresClient = createNumbers(context, 21, 22, 23);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.selectStream(trans, "SELECT i FROM numbers WHERE i IN (21, 23, 25) ORDER BY i",
          Tuple.tuple(), 1, context.asyncAssertSuccess(select -> {
            intsAsString(select, context.asyncAssertSuccess(string -> {
              postgresClient.endTx(trans, context.asyncAssertSuccess());
              context.assertEquals("21, 23", string);
            }));
          }));
    }));
  }

  @Test
  public void selectStreamParamTrans(TestContext context) {
    postgresClient = createNumbers(context, 31, 32, 33);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.selectStream(trans, "SELECT i FROM numbers WHERE i IN ($1, $2, $3) ORDER BY i",
          Tuple.of(31, 33, 35),
          context.asyncAssertSuccess(select -> {
            intsAsString(select, context.asyncAssertSuccess(string -> {
              postgresClient.endTx(trans, context.asyncAssertSuccess());
              context.assertEquals("31, 33", string);
            }));
          }));
    }));
  }

  @Test
  public void selectStreamParamSyntaxError(TestContext context) {
    postgresClient = createNumbers(context, 31, 32, 33);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.selectStream(trans, "SELECT (", Tuple.tuple(),
          context.asyncAssertFailure(select -> {
            assertThat(new PgExceptionFacade(select).getSqlState(), is("42601")); // syntax error
            postgresClient.endTx(trans, context.asyncAssertFailure(endTx -> {
              assertThat(endTx, is(instanceOf(TransactionRollbackException.class)));
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
  public void selectSingleFuture(TestContext context) {
    postgresClient = createNumbers(context, 41, 42, 43);
    postgresClient.selectSingle("SELECT i FROM numbers WHERE i IN (41, 43, 45) ORDER BY i")
        .onComplete(context.asyncAssertSuccess(select -> {
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
    postgresClient.selectSingle("SELECT i FROM numbers WHERE i IN ($1, $2, $3) ORDER BY i",
        Tuple.of(51, 53, 55),
        context.asyncAssertSuccess(select -> {
          context.assertEquals(51, select.getInteger(0));
        }));
  }

  @Test
  public void selectSingleParamFuture(TestContext context) {
    postgresClient = createNumbers(context, 51, 52, 53);
    postgresClient.selectSingle("SELECT i FROM numbers WHERE i IN ($1, $2, $3) ORDER BY i",
        Tuple.of(51, 53, 55)).onComplete(context.asyncAssertSuccess(select -> {
          context.assertEquals(51, select.getInteger(0));
        }));
  }

  @Test
  public void selectSingleParamSyntaxError(TestContext context) {
    postgresClient = createNumbers(context, 51, 52, 53);
    postgresClient.selectSingle("SELECT (",
        Tuple.tuple(), context.asyncAssertFailure());
  }

  @Test
  public void selectSingleParamTrans(TestContext context) {
    postgresClient = createNumbers(context, 55, 56, 57);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient.selectSingle(trans, "SELECT i FROM numbers WHERE i IN ($1, $2, $3) ORDER BY i",
          Tuple.of(51, 53, 55),
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
    postgresClient().select(null, "SELECT 1", Tuple.tuple(), context.asyncAssertFailure());
  }

  @Test
  public void selectSingleTxException(TestContext context) {
    postgresClient().selectSingle(null, "SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void selectSingleParamTxException(TestContext context) {
    postgresClient().selectSingle(null, "SELECT 1", Tuple.tuple(), context.asyncAssertFailure());
  }

  @Test
  public void selectStreamNullConnection(TestContext context) {
    new Conn(null, null).selectStream("sql", Tuple.tuple(), 1, rowStreamHandler -> {})
    .onComplete(context.asyncAssertFailure(e -> assertThat(e, is(instanceOf(NullPointerException.class)))));
  }

  @Test
  public void selectStreamTxException(TestContext context) {
    postgresClient().selectStream(null, "SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void selectStreamParamTxException(TestContext context) {
    postgresClient().selectStream(null, "SELECT 1", Tuple.tuple(), context.asyncAssertFailure());
  }

  @Test
  public void selectStreamParamTxSqlError(TestContext context) {
    postgresClient = createNumbers(context, 55, 56, 57);
    postgresClient.startTx(asyncAssertTx(context, trans -> {
      postgresClient().selectStream(trans, "sql", Tuple.tuple(), context.asyncAssertFailure());
    }));
  }

  @Test
  public void selectTxFailed(TestContext context) {
    postgresClient().select(Future.failedFuture("failed"), "SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void selectParamTxFailed(TestContext context) {
    postgresClient().select(Future.failedFuture("failed"), "SELECT 1", Tuple.tuple(), context.asyncAssertFailure());
  }

  @Test
  public void selectSingleTxFailed(TestContext context) {
    postgresClient().selectSingle(Future.failedFuture("failed"), "SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void selectSingleParamTxFailed(TestContext context) {
    postgresClient().selectSingle(Future.failedFuture("failed"), "SELECT 1", Tuple.tuple(), context.asyncAssertFailure());
  }

  @Test
  public void selectStreamTxFailed(TestContext context) {
    postgresClient().selectStream(Future.failedFuture("failed"), "SELECT 1", context.asyncAssertFailure());
  }

  @Test
  public void selectStreamParamTxFailed(TestContext context) {
    postgresClient().selectStream(Future.failedFuture("failed"), "SELECT 1", Tuple.tuple(), context.asyncAssertFailure());
  }

  @Test
  public void selectDistinctOn(TestContext context) throws IOException {
    Async async = context.async();
    createTableWithPoLines(context);
    postgresClient.select("SELECT DISTINCT ON (jsonb->>'owner') * FROM mock_po_lines  ORDER BY (jsonb->>'owner') DESC", select -> {
      context.assertEquals(3, select.result().size());
      async.complete();
    });
    async.awaitSuccess();
  }

  @Test
  public void streamGetLegacy(TestContext context) throws IOException {
    AtomicInteger objectCount = new AtomicInteger();
    createTableWithPoLines(context);
    postgresClient.streamGet(MOCK_POLINES_TABLE, new Object(), "jsonb", null, false, null,
      streamHandler -> objectCount.incrementAndGet(), context.asyncAssertSuccess(asyncResult ->
        context.assertEquals(6, objectCount.get())));
  }

  @Test
  public void streamGetLegacyFilter(TestContext context) throws IOException, FieldException {
    AtomicInteger objectCount = new AtomicInteger();
    createTableWithPoLines(context);
    postgresClient.streamGet(MOCK_POLINES_TABLE, new Object(), "jsonb", firstEdition(), false, null,
      streamHandler -> objectCount.incrementAndGet(), context.asyncAssertSuccess(asyncResult ->
        context.assertEquals(3, objectCount.get())));
  }

  @Test
  public void streamGetLegacySyntaxError(TestContext context) throws IOException, FieldException {
    createTableWithPoLines(context);
    CQLWrapper wrapper = new CQLWrapper(new CQL2PgJSON("jsonb"), "edition=");
    postgresClient.streamGet(MOCK_POLINES_TABLE, new Object(), "jsonb", wrapper,
      false, null, streamHandler -> context.fail(), context.asyncAssertFailure());
  }

  @Test
  public void streamGetLegacyQuerySingleError(TestContext context) throws IOException, FieldException {
    createTableWithPoLines(context);
    AtomicInteger objectCount = new AtomicInteger();
    postgresClient.streamGet("noSuchTable", new Object(), "jsonb", firstEdition(),
      false, null, streamHandler -> objectCount.incrementAndGet(),
      context.asyncAssertFailure(asyncResult
        -> context.assertEquals(0, objectCount.get())));
  }

  @Test
  public void streamGetQuerySingleError(TestContext context) throws IOException, FieldException {
    createTableWithPoLines(context);
    postgresClient.streamGet("noSuchTable", Object.class, "jsonb", firstEdition(),
      false, null, context.asyncAssertFailure());
  }

  @Test
  public void streamGetFilterNoHandlers(TestContext context) throws IOException, FieldException {
    createTableWithPoLines(context);
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", firstEdition(),
      false, null, context.asyncAssertSuccess(sr -> {
        context.assertEquals(3, sr.resultInfo().getTotalRecords());
      }));
  }

  @Test
  public void streamGetWithFilterHandlers(TestContext context) throws IOException, FieldException {
    AtomicInteger objectCount = new AtomicInteger();
    Async async = context.async();
    createTableWithPoLines(context);
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", firstEdition(), false, null,
      context.asyncAssertSuccess(sr -> {
        context.assertEquals(3, sr.resultInfo().getTotalRecords());
        sr.handler(streamHandler -> objectCount.incrementAndGet());
        sr.endHandler(x -> {
          context.assertEquals(3, objectCount.get());
          async.complete();
        });
      }));
  }

  @Test
  public void closeAtEndException(TestContext context) throws Exception {
    Async async = context.async();
    CQLWrapper cql = new CQLWrapper(new CQL2PgJSON("jsonb"), "id=*", 1, /* offset */ 0);
    postgresClient = createTable(context, TENANT, MOCK_POLINES_TABLE, "id UUID PRIMARY KEY, jsonb JSONB NOT NULL");
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", cql, false, null, 0,
        asyncResult -> {
          if (asyncResult.succeeded()) {
            throw new RuntimeException("foo");
          } else {
            context.assertEquals("foo", asyncResult.cause().getMessage());
            async.complete();
          }
        });
  }

  private void streamGetCursorPage(TestContext context, int limit) throws Exception {
    Async async = context.async();
    CQLWrapper cql = new CQLWrapper(new CQL2PgJSON("jsonb"), "id=*", limit, /* offset */ 0);
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", cql, false, null,
        context.asyncAssertSuccess(r -> {
          AtomicInteger count = new AtomicInteger();
          r.handler(streamHandler -> {
            count.incrementAndGet();
          });
          r.endHandler(x -> {
            context.assertEquals(limit, count.get());
            async.complete();
          });
          r.exceptionHandler(e -> context.fail(e));
        }));
  }

  @Test
  public void streamGetCursorPage(TestContext context) throws Exception {
    String schema = PostgresClient.convertToPsqlStandard(TENANT);
    postgresClient = createTable(context, TENANT, MOCK_POLINES_TABLE, "id UUID PRIMARY KEY, jsonb JSONB NOT NULL");
    int n = PostgresClient.STREAM_GET_DEFAULT_CHUNK_SIZE;
    int max = 2*n+1;
    execute(context,
        "INSERT INTO " + schema + "." + MOCK_POLINES_TABLE +
        " SELECT id, jsonb_build_object('id', id) as json" +
        " FROM (SELECT md5(generate_series(1, " + max + ")::text)::uuid) x(id)");
    // check that it works at the borders of the pages (chunks) we use for the SQL cursor
    streamGetCursorPage(context, 0);
    streamGetCursorPage(context, 1);
    streamGetCursorPage(context, n-1);
    streamGetCursorPage(context, n);
    streamGetCursorPage(context, n+1);
    streamGetCursorPage(context, 2*n-1);
    streamGetCursorPage(context, 2*n);
    streamGetCursorPage(context, 2*n+1);
  }

  @Test
  public void streamGetUnsupported(TestContext context) throws IOException, FieldException {
    createTableWithPoLines(context);
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", firstEdition(), false, null,
      context.asyncAssertSuccess(sr -> {
        try {
          sr.pause();
          context.fail();
        } catch (Exception ex) {
          context.assertEquals("Not supported yet: pause", ex.getMessage());
        }
        try {
          sr.resume();
          context.fail();
        } catch (Exception ex) {
          context.assertEquals("Not supported yet: resume", ex.getMessage());
        }
        try {
          sr.fetch(0);
          context.fail();
        } catch (Exception ex) {
          context.assertEquals("Not supported yet: fetch", ex.getMessage());
        }
      }));
  }

  @Test
  public void streamGetWithFilterZeroHits(TestContext context) throws IOException, FieldException {
    AtomicInteger objectCount = new AtomicInteger();
    Async async = context.async();
    createTableWithPoLines(context);
    CQLWrapper wrapper = new CQLWrapper(new CQL2PgJSON("jsonb"), "edition=Millenium edition");
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", wrapper, false, null,
      context.asyncAssertSuccess(sr -> {
        context.assertEquals(0, sr.resultInfo().getTotalRecords());
        sr.handler(streamHandler -> objectCount.incrementAndGet());
        sr.endHandler(x -> {
          context.assertEquals(0, objectCount.get());
          async.complete();
        });
      }));
    async.awaitSuccess();
  }

  @Test
  public void streamGetWithFacetsAndFilter(TestContext context) throws IOException, FieldException {
    AtomicInteger objectCount = new AtomicInteger();
    Async async = context.async();
    List<FacetField> facets = new ArrayList<FacetField>();
    facets.add(new FacetField("jsonb->>'edition'"));
    facets.add(new FacetField("jsonb->>'title'"));
    createTableWithPoLines(context);
    postgresClient.streamGet(MOCK_POLINES_TABLE, Poline.class, "jsonb", firstEdition(), true, null,
      facets, QUERY_TIMEOUT, context.asyncAssertSuccess(sr -> {
        ResultInfo resultInfo = sr.resultInfo();
        context.assertEquals(3, resultInfo.getTotalRecords());
        context.assertEquals(2, resultInfo.getFacets().size());
        context.assertEquals("edition", resultInfo.getFacets().get(0).getType());
        context.assertEquals(1, resultInfo.getFacets().get(0).getFacetValues().size());
        context.assertEquals("First edition", resultInfo.getFacets().get(0).getFacetValues().get(0).getValue());
        context.assertEquals(3, resultInfo.getFacets().get(0).getFacetValues().get(0).getCount());
        context.assertEquals("title", resultInfo.getFacets().get(1).getType());
        context.assertEquals(3, resultInfo.getFacets().get(1).getFacetValues().size());
        context.assertEquals(1, resultInfo.getFacets().get(1).getFacetValues().get(0).getCount());
        context.assertEquals(1, resultInfo.getFacets().get(1).getFacetValues().get(1).getCount());
        context.assertEquals(1, resultInfo.getFacets().get(1).getFacetValues().get(2).getCount());
        sr.handler(streamHandler -> objectCount.incrementAndGet());
        sr.endHandler(x -> {
          context.assertEquals(3, objectCount.get());
          async.complete();
        });
      }));
    async.awaitSuccess();
  }

  @Test
  public void normalGetWithFacetsAndFilter(TestContext context) throws IOException, FieldException {
    List<FacetField> facets = new ArrayList<FacetField>();
    facets.add(new FacetField("jsonb->>'edition'"));
    facets.add(new FacetField("jsonb->>'title'"));
    createTableWithPoLines(context);
    postgresClient.get(MOCK_POLINES_TABLE, Poline.class, new String[]{"jsonb"}, firstEdition(),
        true, true, facets, context.asyncAssertSuccess(sr -> {
      ResultInfo resultInfo = sr.getResultInfo();
      context.assertEquals(3, resultInfo.getTotalRecords());
      context.assertEquals(2, resultInfo.getFacets().size());
      context.assertEquals("edition", resultInfo.getFacets().get(0).getType());
      context.assertEquals(1, resultInfo.getFacets().get(0).getFacetValues().size());
      context.assertEquals("First edition", resultInfo.getFacets().get(0).getFacetValues().get(0).getValue());
      context.assertEquals(3, resultInfo.getFacets().get(0).getFacetValues().get(0).getCount());
      context.assertEquals("title", resultInfo.getFacets().get(1).getType());
      context.assertEquals(3, resultInfo.getFacets().get(1).getFacetValues().size());
      context.assertEquals(1, resultInfo.getFacets().get(1).getFacetValues().get(0).getCount());
      context.assertEquals(1, resultInfo.getFacets().get(1).getFacetValues().get(1).getCount());
      context.assertEquals(1, resultInfo.getFacets().get(1).getFacetValues().get(2).getCount());
    }));
  }

  @Test
  public void streamGetWithFacetsZeroHits(TestContext context) throws IOException, FieldException {
    AtomicInteger objectCount = new AtomicInteger();
    Async async = context.async();
    List<FacetField> facets = new ArrayList<FacetField>();
    facets.add(new FacetField("jsonb->>'edition'"));
    facets.add(new FacetField("jsonb->>'title'"));
    createTableWithPoLines(context);
    CQLWrapper wrapper = new CQLWrapper(new CQL2PgJSON("jsonb"), "edition=Millenium edition");
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", wrapper, true, null,
      facets, context.asyncAssertSuccess(sr -> {
        ResultInfo resultInfo = sr.resultInfo();
        context.assertEquals(0, resultInfo.getTotalRecords());
        context.assertEquals(0, resultInfo.getFacets().size());
        sr.handler(streamHandler -> objectCount.incrementAndGet());
        sr.endHandler(x -> {
          context.assertEquals(0, objectCount.get());
          async.complete();
        });
      }));
    async.awaitSuccess();
  }

  @Test
  public void streamGetWithFacetsError(TestContext context) throws IOException, FieldException {
    List<FacetField> badFacets = new ArrayList<FacetField>();
    badFacets.add(new FacetField("'"));  // bad facet
    createTableWithPoLines(context);
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", firstEdition(), true, null,
      badFacets, QUERY_TIMEOUT, context.asyncAssertFailure());
  }

  @Test
  public void streamGetWithSyntaxError(TestContext context) throws IOException, FieldException {
    createTableWithPoLines(context);
    CQLWrapper wrapper = new CQLWrapper(new CQL2PgJSON("jsonb"), "edition=");
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", wrapper, false, null,
      context.asyncAssertFailure());
  }

  @Test
  public void streamGetExceptionInHandler(TestContext context) throws IOException, FieldException {
    createTableWithPoLines(context);
    StringBuilder events = new StringBuilder();
    Async async = context.async();
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", firstEdition(), false, null,
      context.asyncAssertSuccess(sr -> {
        sr.handler(streamHandler -> {
          events.append("[handler]");
          throw new NullPointerException("null");
        });
        sr.exceptionHandler(x -> {
          events.append("[exception]");
          vertx.runOnContext(run -> async.complete());
        });
        sr.endHandler(x -> {
          events.append("[endHandler]");
        });
      }));
    async.await(1000);
    context.assertEquals("[handler][exception]", events.toString());
  }

  @Test
  public void streamGetConnectionFailed(TestContext context) throws IOException, FieldException {
    createTableWithPoLines(context);
    List<FacetField> facets = new ArrayList<FacetField>();
    AsyncResult<SQLConnection> connResult = Future.failedFuture("connection error");
    postgresClient.streamGet(connResult, MOCK_POLINES_TABLE, Object.class, "jsonb", firstEdition(), true,
      null, facets, context.asyncAssertFailure(
        x -> context.assertEquals("connection error", x.getMessage())));
  }

  class MySQLRowStream implements RowStream<Row> {

    @Override
    public RowStream<Row> exceptionHandler(Handler<Throwable> handler) {
      vertx.runOnContext(x -> handler.handle(new Throwable("SQLRowStream exception")));
      return this;
    }

    @Override
    public RowStream<Row> handler(Handler<Row> handler) {
      return this;
    }

    @Override
    public RowStream<Row> pause() {
      return this;
    }

    @Override
    public RowStream<Row> resume() {
      return null;
    }

    @Override
    public RowStream<Row> fetch(long l) {
      return this;
    }

    @Override
    public RowStream<Row> endHandler(Handler<Void> handler) {
      return this;
    }

    @Override
    public Future<Void> close() {
      return Future.succeededFuture();
    }

    @Override
    public void close(Handler<AsyncResult<Void>> handler) {

    }
  }

  @Test
  public void streamGetResultException(TestContext context) throws IOException, FieldException {
    List<FacetField> facets = new ArrayList<FacetField>();
    createTableWithPoLines(context);
    ResultInfo resultInfo = new ResultInfo();
    context.assertNotNull(vertx);
    RowStream<Row> sqlRowStream = new MySQLRowStream();
    StringBuilder events = new StringBuilder();
    Async async = context.async();
    PostgresClientStreamResult<Object> streamResult = new PostgresClientStreamResult(resultInfo);
    Transaction transaction = null;
    postgresClient.doStreamRowResults(sqlRowStream, Object.class, transaction,
      new QueryHelper("table_name"), streamResult, context.asyncAssertSuccess(sr -> {
        sr.handler(streamHandler -> {
          events.append("[handler]");
        });
        sr.endHandler(x -> {
          events.append("[endHandler]");
          throw new NullPointerException("null");
        });
        sr.exceptionHandler(x -> {
          events.append("[exception]");
          context.assertEquals("SQLRowStream exception", x.getMessage());
          async.complete();
        });
      }));
    async.await(1000);
    context.assertEquals("[exception]", events.toString());
  }


  @Test
  public void streamGetExceptionInEndHandler(TestContext context) throws IOException, FieldException {
    createTableWithPoLines(context);
    StringBuilder events = new StringBuilder();
    Async async = context.async();
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", firstEdition(), false, null,
      context.asyncAssertSuccess(sr -> {
        sr.handler(streamHandler -> {
          events.append("[handler]");
        });
        sr.endHandler(x -> {
          events.append("[endHandler]");
          throw new NullPointerException("null");
        });
        sr.exceptionHandler(x -> {
          events.append("[exception]");
          async.complete();
        });
      }));
    async.await(1000);
    context.assertEquals("[handler][handler][handler][endHandler][exception]", events.toString());
  }

  @Test
  public void streamGetExceptionInEndHandler2(TestContext context) throws IOException, FieldException {
    createTableWithPoLines(context);
    StringBuilder events = new StringBuilder();
    Async async = context.async();
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", firstEdition(), false, null,
      context.asyncAssertSuccess(sr -> {
        sr.handler(streamHandler -> {
          events.append("[handler]");
        });
        sr.endHandler(x -> {
          events.append("[endHandler]");
          throw new NullPointerException("null");
        });
        // no exceptionHandler defined
        vertx.setTimer(100, x -> async.complete());
      }));
    async.await(1000);
    context.assertEquals("[handler][handler][handler][endHandler]", events.toString());
  }

  @Test
  public void streamGetExceptionInHandler2(TestContext context) throws IOException, FieldException {
    createTableWithPoLines(context);
    StringBuilder events = new StringBuilder();
    Async async = context.async();
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", firstEdition(), false, null,
      context.asyncAssertSuccess(sr -> {
        sr.handler(streamHandler -> {
          events.append("[handler]");
          vertx.runOnContext(run -> async.complete());
          throw new NullPointerException("null");
        });
        sr.endHandler(x -> {
          events.append("[endHandler]");
        });
        // no exceptionHandler defined
      }));
    async.await(1000);
    context.assertEquals("[handler]", events.toString());
  }

  @Test
  public void streamGetExceptionInHandler3(TestContext context) throws IOException, FieldException {
    createTableWithPoLines(context);
    StringBuilder events = new StringBuilder();
    Async async = context.async();
    postgresClient.streamGet(MOCK_POLINES_TABLE, StringBuilder.class /* no JSON mapping */,
      "jsonb", firstEdition(), false, null, context.asyncAssertSuccess(sr -> {
        sr.handler(streamHandler -> {
          events.append("[handler]");
        }).endHandler(x -> {
          events.append("[endHandler]");
        }).exceptionHandler(x -> {
          events.append("[exception]");
          vertx.runOnContext(run -> async.complete());
        });
      }));
    async.await(1000);
    context.assertEquals("[exception]", events.toString());
  }

  @Test
  public void streamGetExceptionInHandler4(TestContext context) throws IOException, FieldException {
    createTableWithPoLines(context);
    StringBuilder events = new StringBuilder();
    Async async = context.async();
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", firstEdition(), false, null,
        context.asyncAssertSuccess(sr -> {
          sr.handler(streamHandler -> {
            events.append("[handler]");
            throw new NullPointerException("null");
          });
          sr.exceptionHandler(x -> {
            events.append("[exception]");
            vertx.runOnContext(run -> async.complete());
          });
          sr.endHandler(x -> {
            events.append("[endHandler]");
          });
        }));
    async.await(1000);
    context.assertEquals("[handler][exception]", events.toString());
  }

  @Test
  public void streamGetWithLimit(TestContext context) throws IOException, FieldException {
    AtomicInteger objectCount = new AtomicInteger();
    createTableWithPoLines(context);
    Async async = context.async();
    CQLWrapper wrapper = firstEdition().setLimit(new Limit(1));
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", wrapper,
      false, null, context.asyncAssertSuccess(sr -> {
        context.assertEquals(3, sr.resultInfo().getTotalRecords());
        sr.handler(streamHandler -> objectCount.incrementAndGet());
        sr.endHandler(x -> {
          context.assertEquals(1, objectCount.get());
          async.complete();
        });
      }));
    async.await(1000);
  }

  @Test
  public void streamGetWithLimitZero(TestContext context) throws IOException, FieldException {
    AtomicInteger objectCount = new AtomicInteger();
    createTableWithPoLines(context);
    Async async = context.async();
    CQLWrapper wrapper = firstEdition().setLimit(new Limit(0));
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", wrapper,
      false, null, context.asyncAssertSuccess(sr -> {
        context.assertEquals(3, sr.resultInfo().getTotalRecords());
        sr.handler(streamHandler -> objectCount.incrementAndGet());
        sr.endHandler(x -> {
          context.assertEquals(0, objectCount.get());
          async.complete();
        });
      }));
    async.await(1000);
  }

  @Test
  public void streamGetPlain(TestContext context) throws IOException, FieldException {
    AtomicInteger objectCount = new AtomicInteger();
    Async async = context.async();
    createTableWithPoLines(context);
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", firstEdition(),
        false, null, context.asyncAssertSuccess(sr -> {
          context.assertEquals(3, sr.resultInfo().getTotalRecords());
          sr.handler(streamHandler -> objectCount.incrementAndGet());
          sr.endHandler(x -> {
            context.assertEquals(3, objectCount.get());
            async.complete();
          });
        }));
    async.await(1000);
  }

  @Test
  public void streamGetWithTransaction(TestContext context) throws IOException, FieldException {
    AtomicInteger objectCount = new AtomicInteger();
    Async async = context.async();
    createTableWithPoLines(context);
    postgresClient.startTx(trans -> {
      postgresClient.streamGet(trans, MOCK_POLINES_TABLE, Object.class, "jsonb", firstEdition(),
          false, null, null, context.asyncAssertSuccess(sr -> {
            context.assertEquals(3, sr.resultInfo().getTotalRecords());
            sr.handler(streamHandler -> objectCount.incrementAndGet());
            sr.endHandler(x -> {
              context.assertEquals(3, objectCount.get());
              postgresClient.endTx(trans, y -> async.complete());
            });

      }));
    });
    async.await(1000);
  }

  @Test
  public void streamGetWithOffset(TestContext context) throws IOException, FieldException {
    AtomicInteger objectCount = new AtomicInteger();
    Async async = context.async();

    createTableWithPoLines(context);
    CQLWrapper wrapper = firstEdition().setOffset(new Offset(1));
    postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", wrapper,
      false, null, context.asyncAssertSuccess(sr -> {
        context.assertEquals(3, sr.resultInfo().getTotalRecords());
        sr.handler(streamHandler -> objectCount.incrementAndGet());
        sr.endHandler(x -> {
          context.assertEquals(2, objectCount.get());
          async.complete();
        });
      }));
    async.await(1000);
  }

  @Test
  public void streamGetWithOffsetAndLimit(TestContext context) throws IOException, FieldException {
    createTableWithPoLines(context);
    Set<String> ids = new HashSet<>();
    for (int i = 0; i < 4; i++) {
      AtomicInteger objectCount = new AtomicInteger();
      Async async = context.async();

      CQLWrapper wrapper = firstEdition().setOffset(new Offset(i)).setLimit(new Limit(1));
      postgresClient.streamGet(MOCK_POLINES_TABLE, Object.class, "jsonb", wrapper,
        false, null, context.asyncAssertSuccess(sr -> {
          context.assertEquals(3, sr.resultInfo().getTotalRecords());
          sr.handler(obj -> {
            try {
              ObjectMapper mapper = new ObjectMapper();
              ids.add(new JsonObject(mapper.writeValueAsString(obj)).getString("id"));
              objectCount.incrementAndGet();
            } catch (JsonProcessingException ex) {
              context.fail(ex);
            }
          });
          sr.endHandler(x -> async.complete());
          sr.exceptionHandler(x -> context.fail(x));
        }));
      async.await(1000);
      // expect when in-bounds; 0 when out of bounds
      context.assertEquals(i < 3 ? 1 : 0, objectCount.get());
    }
    context.assertEquals(3, ids.size());
  }

  @Test
  public void streamGetLegacyDistinctOn(TestContext context) throws IOException {
    AtomicInteger objectCount = new AtomicInteger();
    createTableWithPoLines(context);
    postgresClient.streamGet(MOCK_POLINES_TABLE, new Object(), "jsonb", null, false, "jsonb->>'edition'",
      streamHandler -> objectCount.incrementAndGet(),
      context.asyncAssertSuccess(res -> context.assertEquals(2, objectCount.get())));
  }

  @Test
  public void streamGetFuture4Args(TestContext context) throws Exception {
    createTableWithPoLines(context);
    postgresClient
    .streamGet(MOCK_POLINES_TABLE, Object.class, firstEdition(), context.asyncAssertSuccess(
        r -> assertThat(r.resultInfo().getTotalRecords(), is(3))))
    .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void streamGetFuture8Args(TestContext context) throws Exception {
    createTableWithPoLines(context);
    postgresClient
    .streamGet(MOCK_POLINES_TABLE, Object.class, firstEdition(), "jsonb", false, null, null,
        context.asyncAssertSuccess(r -> assertThat(r.resultInfo().getTotalRecords(), is(3))))
    .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void getDistinctOn(TestContext context) throws IOException {
    Async async = context.async();
    final String tableDefiniton = "id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";
    createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);

    String distinctOn = "jsonb->>'order_format'";
    postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*", "", false, false,
      false, null, distinctOn, handler -> {
        ResultInfo resultInfo = handler.result().getResultInfo();
        context.assertEquals(4, resultInfo.getTotalRecords());
        async.complete();
      });
    async.awaitSuccess();

    String whereClause = "WHERE jsonb->>'order_format' = 'Other'";
    Async async2 = context.async();
    postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*", whereClause, false, false,
      false, null, distinctOn, handler -> {
        ResultInfo resultInfo = handler.result().getResultInfo();
        context.assertEquals(1, resultInfo.getTotalRecords());
        try {
          List<Object> objs = handler.result().getResults();
          ObjectMapper mapper = new ObjectMapper();
          context.assertEquals("70fb4e66-cdf1-11e8-a8d5-f2801f1b9fd1",
            new JsonObject(mapper.writeValueAsString(objs.get(0))).getString("id"));
        } catch (Exception ex) {
          context.fail(ex);
        }
        async2.complete();
      });
    async2.awaitSuccess();
  }

  @Test
  public void getDistinctOnWithFacets(TestContext context) throws IOException, FieldException  {
    final String tableDefiniton = "id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";
    createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);

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
        try {
          ResultInfo resultInfo = handler.result().getResultInfo();
          context.assertEquals(1, resultInfo.getTotalRecords());
          List<Object> objs = handler.result().getResults();
          ObjectMapper mapper = new ObjectMapper();
          context.assertEquals("70fb4e66-cdf1-11e8-a8d5-f2801f1b9fd1",
            new JsonObject(mapper.writeValueAsString(objs.get(0))).getString("id"));

          List<Facet> retFacets = resultInfo.getFacets();
          context.assertEquals(1, retFacets.size());
          context.assertEquals("edition", retFacets.get(0).getType());
          context.assertEquals(1, retFacets.get(0).getFacetValues().size());
          context.assertEquals(1, retFacets.get(0).getFacetValues().get(0).getCount());
          context.assertEquals("First edition", retFacets.get(0).getFacetValues().get(0).getValue());
        } catch (Exception ex) {
          context.fail(ex);
        }
        async2.complete();
      });
    async2.awaitSuccess();
  }

  private void assertCQLWrapper(TestContext context, Function<CQLWrapper,Future<Results<StringPojo>>> function) {
    try {
      JsonArray ids = new JsonArray().add(randomUuid()).add(randomUuid());
      insertXAndSingleQuotePojo(context, ids);
      CQLWrapper cqlWrapper = new CQLWrapper(new CQL2PgJSON("jsonb"), "key = x");
      function.apply(cqlWrapper)
      .onComplete(context.asyncAssertSuccess(res -> {
        assertThat(res.getResults().size(), is(1));
        assertThat(res.getResults().get(0).getId(), is(ids.getString(0)));
      }));
    } catch (FieldException e) {
      context.fail(e);
    }
  }

  @Test
  public void getCQLWrapper(TestContext context) throws CQL2PgJSONException {
    assertCQLWrapper(context, cqlWrapper ->
    postgresClient.get(FOO, StringPojo.class, new String [] { "jsonb" }, cqlWrapper, false, false, (List<FacetField>)null));
  }

  @Test
  public void getCQLWrapper2(TestContext context) throws CQL2PgJSONException {
    assertCQLWrapper(context, cqlWrapper ->
    postgresClient.get(FOO, StringPojo.class, new String [] { "jsonb" }, cqlWrapper, false));
  }

  @Test
  public void getCQLWrapper3(TestContext context) throws CQL2PgJSONException {
    assertCQLWrapper(context, cqlWrapper ->
    postgresClient.get(FOO, StringPojo.class, cqlWrapper, false, (List<FacetField>)null));
  }

  @Test
  public void getCQLWrapper4(TestContext context) throws CQL2PgJSONException {
    assertCQLWrapper(context, cqlWrapper ->
    postgresClient.get(FOO, StringPojo.class, cqlWrapper, false));
  }

  @Test
  public void getCQLWrapper5(TestContext context) throws CQL2PgJSONException {
    assertCQLWrapper(context, cqlWrapper ->
    postgresClient.withConn(conn -> conn.get(FOO, StringPojo.class, cqlWrapper)));
  }

  @Test
  public void getCQLWrapperFailure(TestContext context) throws IOException, FieldException {
    final String tableDefiniton = "id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";
    createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);

    CQL2PgJSON cql2pgJson = new CQL2PgJSON("jsonb");
    {
      CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson,
        "cql.allRecords="); // syntax error
      Async async = context.async();
      postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*",
        cqlWrapper, true, true, null, null/*facets*/, handler -> {
          context.assertTrue(handler.failed());
          async.complete();
        });
      async.awaitSuccess();
    }
  }

  @Test
  public void getCQLWrapperNoCount(TestContext context) throws IOException, FieldException {
    final String tableDefiniton = "id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";
    createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);

    CQL2PgJSON cql2pgJson = new CQL2PgJSON("jsonb");
    {
      CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, "cql.allRecords=1");
      Async async = context.async();
      postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*",
        cqlWrapper, false, true, null, null/*facets*/, handler -> {
          context.assertTrue(handler.succeeded());
          ResultInfo resultInfo = handler.result().getResultInfo();
          context.assertEquals(6, resultInfo.getTotalRecords());
          async.complete();
        });
      async.awaitSuccess();
    }
  }

  @Test
  public void getCQLWrapperJsonbField(TestContext context) throws IOException, FieldException {
    final String tableDefiniton = "id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";
    List<FacetField> facets = null;
    createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);
    CQL2PgJSON cql2pgJson = new CQL2PgJSON(MOCK_POLINES_TABLE + ".jsonb");
    {
      CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, "cql.allRecords=1");
      Async async = context.async();
      postgresClient.get(MOCK_POLINES_TABLE, Object.class, "jsonb",
        cqlWrapper, true, true, facets, null, handler -> {
          context.assertTrue(handler.succeeded());
          ResultInfo resultInfo = handler.result().getResultInfo();
          context.assertEquals(6, resultInfo.getTotalRecords());
          async.complete();
        });
      async.awaitSuccess();
    }
    String distinctOn = "jsonb->>'order_format'";
    {
      CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, "cql.allRecords=1");
      Async async = context.async();
      postgresClient.get(MOCK_POLINES_TABLE, Object.class, "jsonb",
        cqlWrapper, true, true, facets, distinctOn, handler -> {
          context.assertTrue(handler.succeeded());
          ResultInfo resultInfo = handler.result().getResultInfo();
          context.assertEquals(4, resultInfo.getTotalRecords());
          async.complete();
        }
      );
      async.awaitSuccess();
    }
  }

  @Test
  public void getCQLWrapperNoFacets(TestContext context) throws IOException, FieldException {
    final String tableDefiniton = "id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";
    createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);
    List<FacetField> facets = null;
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("jsonb");
    {
      CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, "cql.allRecords=1");
      Async async = context.async();
      postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*",
        cqlWrapper, true, true, facets, null, handler -> {
          context.assertTrue(handler.succeeded());
          ResultInfo resultInfo = handler.result().getResultInfo();
          context.assertEquals(6, resultInfo.getTotalRecords());
          List<Facet> retFacets = resultInfo.getFacets();
          context.assertEquals(0, retFacets.size());
          async.complete();
        });
      async.awaitSuccess();
    }
    String distinctOn = "jsonb->>'order_format'";
    List<FacetField> emptyFacets = new ArrayList<FacetField>();
    {
      CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, "cql.allRecords=1");
      Async async = context.async();
      postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*",
        cqlWrapper, true, true, emptyFacets, distinctOn, handler -> {
          context.assertTrue(handler.succeeded());
          ResultInfo resultInfo = handler.result().getResultInfo();
          context.assertEquals(4, resultInfo.getTotalRecords());
          List<Facet> retFacets = resultInfo.getFacets();
          context.assertEquals(0, retFacets.size());
          async.complete();
        });
      async.awaitSuccess();
    }
    {
      Async async = context.async();
      CQLWrapper cqlWrapperNull = null;
      postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*",
        cqlWrapperNull, true, true, facets, null, handler -> {
          context.assertTrue(handler.succeeded());
          ResultInfo resultInfo = handler.result().getResultInfo();
          context.assertEquals(6, resultInfo.getTotalRecords());
          List<Facet> retFacets = resultInfo.getFacets();
          context.assertEquals(0, retFacets.size());
          async.complete();
        });
      async.awaitSuccess();
    }
    {
      Async async = context.async();
      CQLWrapper cqlWrapperNull = null;
      postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*",
        cqlWrapperNull, true, true, facets, distinctOn, handler -> {
          context.assertTrue(handler.succeeded());
          ResultInfo resultInfo = handler.result().getResultInfo();
          context.assertEquals(4, resultInfo.getTotalRecords());
          try {
            List<Object> objs = handler.result().getResults();
            context.assertEquals(4, objs.size());
          } catch (Exception ex) {
            context.fail(ex);
          }
          List<Facet> retFacets = resultInfo.getFacets();
          context.assertEquals(0, retFacets.size());
          async.complete();
        });
      async.awaitSuccess();
    }
    {
      Async async = context.async();
      postgresClient.get(MOCK_POLINES_TABLE, Object.class, new String[]{"*"},
        true, true, 2, 1, handler -> {
          context.assertTrue(handler.succeeded());
          ResultInfo resultInfo = handler.result().getResultInfo();
          context.assertEquals(6, resultInfo.getTotalRecords());
          try {
            List<Class<Object>> objs = handler.result().getResults();
            context.assertEquals(1, objs.size());
          } catch (Exception ex) {
            context.fail(ex);
          }
          List<Facet> retFacets = resultInfo.getFacets();
          context.assertEquals(0, retFacets.size());
          async.complete();
        });
      async.awaitSuccess();
    }
    {
      Async async = context.async();
      postgresClient.get(MOCK_POLINES_TABLE, Object.class, true, true, handler -> {
          context.assertTrue(handler.succeeded());
          ResultInfo resultInfo = handler.result().getResultInfo();
          context.assertEquals(6, resultInfo.getTotalRecords());
          try {
            List<Class<Object>> objs = handler.result().getResults();
            context.assertEquals(6, objs.size());
          } catch (Exception ex) {
            context.fail(ex);
          }
          List<Facet> retFacets = resultInfo.getFacets();
          context.assertEquals(0, retFacets.size());
          async.complete();
        });
      async.awaitSuccess();
    }
  }

  @Test
  public void getCQLWrapperWithFacets(TestContext context) throws IOException, FieldException {
    final String tableDefiniton = "id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";
    createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);

    CQL2PgJSON cql2pgJson = new CQL2PgJSON("jsonb");
    List<FacetField> facets = new ArrayList<FacetField>() {
      {
        add(new FacetField("jsonb->>'edition'"));
      }
    };
    {
      CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, "cql.allRecords=1");
      Async async = context.async();
      postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*",
        cqlWrapper, true, true, facets, null, handler -> {
          context.assertTrue(handler.succeeded());
          ResultInfo resultInfo = handler.result().getResultInfo();
          context.assertEquals(6, resultInfo.getTotalRecords());
          List<Facet> retFacets = resultInfo.getFacets();
          context.assertEquals(1, retFacets.size());
          async.complete();
        });
      async.awaitSuccess();
    }
    String distinctOn = "jsonb->>'order_format'";
    {
      CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, "cql.allRecords=1");
      Async async = context.async();
      postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*",
        cqlWrapper, true, true, facets, distinctOn, handler -> {
          context.assertTrue(handler.succeeded());
          ResultInfo resultInfo = handler.result().getResultInfo();
          context.assertEquals(4, resultInfo.getTotalRecords());
          List<Facet> retFacets = resultInfo.getFacets();
          context.assertEquals(1, retFacets.size());
          async.complete();
        });
      async.awaitSuccess();
    }
    {
      CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, "order_format==Other");
      Async async = context.async();
      postgresClient.get(MOCK_POLINES_TABLE, Object.class, "*",
          cqlWrapper, true, true, facets, distinctOn,
          context.asyncAssertSuccess(res -> {
            ResultInfo resultInfo = res.getResultInfo();
            context.assertEquals(1, resultInfo.getTotalRecords());
            List<Object> objs = res.getResults();
            ObjectMapper mapper = new ObjectMapper();
            List<Facet> retFacets = resultInfo.getFacets();

            context.assertEquals(1, retFacets.size());
            context.assertEquals("edition", retFacets.get(0).getType());
            context.assertEquals(1, retFacets.get(0).getFacetValues().get(0).getCount());
            context.assertEquals("First edition", retFacets.get(0).getFacetValues().get(0).getValue().toString());
            context.assertEquals(1, objs.size());
            try {
              context.assertEquals("70fb4e66-cdf1-11e8-a8d5-f2801f1b9fd1",
                  new JsonObject(mapper.writeValueAsString(objs.get(0))).getString("id"));
            } catch (JsonProcessingException e) {
              context.fail(e);
            }
            async.complete();
          }));
      async.awaitSuccess();
    }
    {
      CQLWrapper cqlWrapper = new CQLWrapper(cql2pgJson, "order_format==Other");
      Async async = context.async();
      postgresClient.get(MOCK_POLINES_TABLE, Poline.class, "*",
          cqlWrapper, true, true, facets, distinctOn,
          context.asyncAssertSuccess(res -> {
            ResultInfo resultInfo = res.getResultInfo();
            context.assertEquals(1, resultInfo.getTotalRecords());
            List<Poline> objs = res.getResults();
            List<Facet> retFacets = resultInfo.getFacets();

            context.assertEquals(1, retFacets.size());
            context.assertEquals("edition", retFacets.get(0).getType());
            context.assertEquals(1, retFacets.get(0).getFacetValues().get(0).getCount());
            context.assertEquals("First edition", retFacets.get(0).getFacetValues().get(0).getValue().toString());
            context.assertEquals(1, objs.size());
            context.assertEquals("70fb4e66-cdf1-11e8-a8d5-f2801f1b9fd1", objs.get(0).getId());
            async.complete();
          }));
      async.awaitSuccess();
    }
  }

  // offset >= estimated total https://issues.folio.org/browse/RMB-684
  @Test
  public void processQueryWithCountBelowOffset(TestContext context) {
    postgresClient = createNumbers(context, 1, 2, 3, 4, 5);
    postgresClient.startTx(context.asyncAssertSuccess(conn -> {
      QueryHelper queryHelper = new QueryHelper("numbers");
      queryHelper.selectQuery = "SELECT i FROM numbers ORDER BY i OFFSET 2";
      queryHelper.offset = 2;
      queryHelper.countQuery = "SELECT 1";  // estimation=1 is below offset=2
      Function<TotaledResults, Results<Integer>> resultSetMapper = totaledResults -> {
        context.verify(verify -> {
          assertThat(totaledResults.estimatedTotal, is(1));
          assertThat(totaledResults.set.size(), is(3));
        });
        return null;
      };
      postgresClient.processQueryWithCount(conn.conn, queryHelper, "statMethod", resultSetMapper)
      .onComplete(context.asyncAssertSuccess());
    }));
  }

  @Test
  public void processQueryWithCountSqlFailure(TestContext context) {
    postgresClient = postgresClient();
    postgresClient.startTx(context.asyncAssertSuccess(conn -> {
      QueryHelper queryHelper = new QueryHelper("table");
      queryHelper.selectQuery = "'";
      queryHelper.countQuery = "'";
      postgresClient.processQueryWithCount(conn.conn, queryHelper, "statMethod", null)
      .onComplete(context.asyncAssertFailure(fail -> {
        assertThat(fail.getMessage(), containsString("unterminated quoted string"));
      }));
    }));
  }

  @Test
  public void testCacheResultOK(TestContext context) {
    createNumbers(context, 1, 2, 3);

    postgresClient.removePersistentCacheResult("cache_numbers_does_not_exist",
        context.asyncAssertFailure());

    postgresClient.persistentlyCacheResult("cache_numbers",
        "SELECT i FROM numbers WHERE i IN (1, 3, 5) ORDER BY i", context.asyncAssertSuccess(
            res -> {
              context.assertEquals(2, res);
              postgresClient.removePersistentCacheResult("cache_numbers",
                  context.asyncAssertSuccess());
            }
        ));
  }

  @Test
  public void testCacheResultCQLOK(TestContext context) throws IOException, FieldException {
    final String tableDefiniton = "id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";

    createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);
    postgresClient.persistentlyCacheResult("cache_polines", MOCK_POLINES_TABLE, firstEdition(),
        context.asyncAssertSuccess(res ->
            postgresClient.removePersistentCacheResult("cache_polines",
                  context.asyncAssertSuccess())
        ));
  }

  @Test
  public void testCacheResultCQLSyntaxError(TestContext context) throws IOException, FieldException {
    final String tableDefiniton = "id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";

    createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);
    CQLWrapper wrapper = new CQLWrapper(new CQL2PgJSON("jsonb"), "edition=");
    postgresClient.persistentlyCacheResult("cache_polines", MOCK_POLINES_TABLE, wrapper,
        context.asyncAssertFailure(res ->
            context.assertTrue(res.getMessage().contains("expected index or term"))));
  }

  @Test
  public void testCacheResultCQLNull(TestContext context) throws IOException, FieldException {
    final String tableDefiniton = "id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";

    createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);
    CQLWrapper wrapper = null;
    postgresClient.persistentlyCacheResult("cache_polines", MOCK_POLINES_TABLE, wrapper,
        context.asyncAssertSuccess(res ->
            postgresClient.removePersistentCacheResult("cache_polines",
                context.asyncAssertSuccess())
        ));
  }

  @Test
  public void testCacheResultCriterion1(TestContext context) throws IOException, FieldException {
    createNumbers(context, 1, 2, 3);

    Criterion criterion = new Criterion();
    criterion.addCriterion(new Criteria().addField("i").setOperation("=").setVal("2").setJSONB(false));

    postgresClient.persistentlyCacheResult("cache_numbers", "numbers", criterion,
        context.asyncAssertSuccess(res ->
            postgresClient.removePersistentCacheResult("cache_numbers",
                context.asyncAssertSuccess())
        ));
  }

  @Test
  public void testCacheResultCriterion2(TestContext context) throws IOException, FieldException {
    createNumbers(context, 1, 2, 3);

    Criterion criterion = null;
    postgresClient.persistentlyCacheResult("cache_numbers", "numbers", criterion,
        context.asyncAssertSuccess(res ->
            postgresClient.removePersistentCacheResult("cache_numbers",
                context.asyncAssertSuccess())
        ));
  }

  @Test
  public void testCacheResultFailure(TestContext context) {
    createNumbers(context, 1, 2, 3);

    postgresClient.persistentlyCacheResult("cache_numbers",
        "SELECT i FROM", context.asyncAssertFailure());

    postgresClient.persistentlyCacheResult(null,
        "SELECT i FROM", context.asyncAssertFailure());

    postgresClientNullConnection().persistentlyCacheResult("cache_numbers",
        "SELECT i FROM", context.asyncAssertFailure());

    postgresClientGetConnectionFails().persistentlyCacheResult("cache_numbers",
        "SELECT i FROM", context.asyncAssertFailure());

    postgresClient.removePersistentCacheResult("cache_numbers_does_not_exist",
        context.asyncAssertFailure());

    postgresClient.removePersistentCacheResult(null,
        context.asyncAssertFailure());

    postgresClientNullConnection().removePersistentCacheResult("cache_numbers",
        context.asyncAssertFailure());

    postgresClientGetConnectionFails().removePersistentCacheResult("cache_numbers",
        context.asyncAssertFailure());
  }

  private void createTableWithPoLines(TestContext context, String tableName, String tableDefiniton) {
    String schema = PostgresClient.convertToPsqlStandard(TENANT);
    String polines = getMockData("mockdata/poLines.json");
    postgresClient = createTable(context, TENANT, tableName, tableDefiniton);
    for (String jsonbValue : polines.split("\n")) {
      String additionalField = new JsonObject(jsonbValue).getString("publication_date");
      execute(context, "INSERT INTO " + schema + "." + tableName + " (id, jsonb, distinct_test_field) VALUES "
        + "('" + randomUuid() + "', '" + jsonbValue + "' ," + additionalField + " ) ON CONFLICT DO NOTHING;");
    }
  }

  private void createTableWithPoLines(TestContext context) {
    final String tableDefiniton = "id UUID PRIMARY KEY , jsonb JSONB NOT NULL, distinct_test_field TEXT";
    createTableWithPoLines(context, MOCK_POLINES_TABLE, tableDefiniton);
  }

  /**
   * Returns {@code new CQLWrapper(new CQL2PgJSON("jsonb"), "edition=\"First edition\"")}
   */
  private static CQLWrapper firstEdition() {
    try {
      return new CQLWrapper(new CQL2PgJSON("jsonb"), "edition=\"First edition\"");
    } catch (FieldException e) {
      throw new RuntimeException(e);
    }
  }

  public static String getMockData(String path) {
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
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  public void testCacheResultCQLWrapper(TestContext context) {
    createNumbers(context, 1, 2, 3);

    postgresClient.removePersistentCacheResult("cache_numbers_does_not_exist",
        context.asyncAssertFailure());

    postgresClient.persistentlyCacheResult("cache_numbers",
        "SELECT i FROM numbers WHERE i IN (1, 3, 5) ORDER BY i", context.asyncAssertSuccess(
            res -> {
              context.assertEquals(2, res);
              postgresClient.removePersistentCacheResult("cache_numbers",
                  context.asyncAssertSuccess());
            }
        ));
  }

  @Test
  public void selectReturnFail(TestContext context) {
    Promise<RowSet<Row>> promise = Promise.promise();
    promise.complete(null);
    PostgresClient.selectReturn(promise.future(), context.asyncAssertFailure());
  }

  @Test
  public void selectReturnEmptySet(TestContext context) {
    RowSet rowSet = new LocalRowSet(0);
    Promise<RowSet<Row>> promise = Promise.promise();
    promise.complete(rowSet);
    PostgresClient.selectReturn(promise.future(), context.asyncAssertSuccess(res ->
      context.assertEquals(null, res)));
  }

  @Test
  public void selectReturnOneRow(TestContext context) {
    List<String> columns = new LinkedList<>();
    columns.add("field");
    RowDesc rowDesc = new RowDesc(columns);
    List<Row> rows = new LinkedList<>();
    Row row = new RowImpl(rowDesc);
    row.addString("value");
    rows.add(row);
    RowSet rowSet = new LocalRowSet(1).withColumns(columns).withRows(rows);

    Promise<RowSet<Row>> promise = Promise.promise();
    promise.complete(rowSet);
    PostgresClient.selectReturn(promise.future(), context.asyncAssertSuccess(res ->
        context.assertEquals("value", res.getString(0))));
  }


}
