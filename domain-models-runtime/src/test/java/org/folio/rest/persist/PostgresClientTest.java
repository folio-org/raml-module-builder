package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.collection.ArrayMatching.arrayContaining;
import static org.hamcrest.collection.ArrayMatching.hasItemInArray;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.text.StringContainsInOrder.stringContainsInOrder;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collector;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.RowImpl;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.impl.RowDesc;
import org.folio.rest.persist.facets.FacetField;
import org.folio.rest.persist.helpers.LocalRowDesc;
import org.folio.rest.persist.helpers.LocalRowSet;
import org.folio.rest.security.AES;
import org.folio.rest.security.AESTest;
import org.folio.rest.tools.utils.Envs;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.util.PostgresTester;
import org.folio.util.ResourceUtil;
import org.folio.rest.jaxrs.model.User;
import org.folio.rest.persist.PostgresClient.QueryHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PostgresClientTest {
  // See PostgresClientIT.java for the tests that require a postgres database!

  private String oldConfigFilePath;
  /** empty = no environment variables */
  private JsonObject empty = new JsonObject();

  private static final int DEFAULT_OFFSET = 0;

  private static final int DEFAULT_LIMIT = 10;

  @Before
  public void initConfig() {
    oldConfigFilePath = PostgresClient.getConfigFilePath();
    PostgresClient.setConfigFilePath(null);
  }

  @After
  public void restoreConfig() {
    PostgresClient.setConfigFilePath(oldConfigFilePath);
  }

  @Test
  public void configDefault() throws Exception {
    PostgresClient.setConfigFilePath("nonexisting");
    JsonObject config = PostgresClient.getPostgreSQLClientConfig(/* default schema = */ "public", null, empty, false);
    assertThat(config.getBoolean("postgres_tester"), is(false));
    assertThat(config.containsKey("host"), is(false));
    assertThat(config.containsKey("port"), is(false));
    assertThat(config.getString("username"), is("username"));
  }

  @Test
  public void configPostgresTesterEnabled() throws Exception {
    JsonObject config = PostgresClient.getPostgreSQLClientConfig(/* default schema = */ "public", null, empty, true);
    assertThat(config.getBoolean("postgres_tester"), is(true));
    assertThat(config.containsKey("host"), is(false));
    assertThat(config.containsKey("port"), is(false));
    assertThat(config.getString("username"), is("username"));
  }

  @Test
  public void configDefaultWithPortAndTenant() throws Exception {
    int port = NetworkUtils.nextFreePort();
    PostgresClient.setConfigFilePath("nonexisting");
    JsonObject config = PostgresClient.getPostgreSQLClientConfig("footenant", "barschema", empty, false);
    assertThat(config.getBoolean("postgres_tester"), is(false));
    assertThat(config.getString("username"), is("barschema"));
  }

  @Test
  public void configEnvironmentPlusFile() throws Exception {
    Long previous = PostgresClient.getExplainQueryThreshold();
    JsonObject env = new JsonObject()
        .put("DB_EXPLAIN_QUERY_THRESHOLD", 1200L)
        .put("host", "example.com")
        .put("port", 9876)
        .put("connectionReleaseDelay", 90000);
    JsonObject config = PostgresClient.getPostgreSQLClientConfig("footenant", "aSchemaName", env, true);
    assertThat(config.getBoolean("postgres_tester"), is(false));
    assertThat(config.getString("host"), is("example.com"));
    assertThat(config.getInteger("port"), is(9876));
    assertThat(config.getString("username"), is("aSchemaName"));
    assertThat(config.getInteger("connectionReleaseDelay"), is(90000));
    assertThat(PostgresClient.getExplainQueryThreshold(), is(1200L));
    PostgresClient.setExplainQueryThreshold(previous);
  }

  @Test
  public void configFile() throws Exception {
    // values from src/test/resources/my-postgres-conf.json
    PostgresClient.setConfigFilePath("/my-postgres-conf.json");
    JsonObject config = PostgresClient.getPostgreSQLClientConfig("public", null, empty, false);
    assertThat(config.getString("host"), is("localhost"));
    assertThat(config.getInteger("port"), is(5433));
    assertThat(config.getString("username"), is("postgres"));
    assertThat(config.getInteger("connectionReleaseDelay"), is(30000));
  }

  @Test
  public void configFileTenant() throws Exception {
    // values from src/test/resources/my-postgres-conf.json
    PostgresClient.setConfigFilePath("/my-postgres-conf.json");
    JsonObject config = PostgresClient.getPostgreSQLClientConfig("footenant", "mySchemaName", empty, false);
    assertThat(config.getString("host"), is("localhost"));
    assertThat(config.getInteger("port"), is(5433));
    assertThat(config.getString("username"), is("mySchemaName"));
    assertThat(config.getInteger("connectionReleaseDelay"), is(30000));
  }

  @Test
  public void getConnectionConfig() throws Exception {
    try {
      Envs.setEnv("somehost", 10001, "someusername", "somepassword", "somedatabase");
      JsonObject json = new PostgresClient(Vertx.vertx(), "public").getConnectionConfig();
      assertThat(json.getString("host"), is("somehost"));
      assertThat(json.getInteger("port"), is(10001));
      assertThat(json.getString("username"), is("someusername"));
      assertThat(json.getString("password"), is("somepassword"));
      assertThat(json.getString("database"), is("somedatabase"));
    } finally {
      // restore defaults
      Envs.setEnv(System.getenv());
    }
  }

  @Test
  public void testPgConnectOptionsEmpty() {
    JsonObject conf = new JsonObject();
    PgConnectOptions options =
        PostgresClientInitializer.createPgConnectOptions(conf, PostgresClient.HOST, PostgresClient.PORT);
    assertThat("localhost", is(options.getHost()));
    assertThat(5432, is(options.getPort()));
    assertThat("user", is(options.getUser()));
    assertThat("pass", is(options.getPassword()));
    assertThat("db", is(options.getDatabase()));
    // TODO: enable when available in vertx-sql-client/vertx-pg-client
    // https://issues.folio.org/browse/RMB-657
    // assertThat(60000, is(options.getConnectionReleaseDelay()));
  }

  @Test
  public void testPgConnectOptionsFull() throws Exception {
    try {
      Envs.setEnv(Map.of(
          "DB_HOST", "myhost",
          "DB_PORT", "5433",
          "DB_USERNAME", "myuser",
          "DB_PASSWORD", "mypassword",
          "DB_DATABASE", "mydatabase",
          "DB_CONNECTIONRELEASEDELAY", "1000",
          "DB_RECONNECTATTEMPTS", "3",
          "DB_RECONNECTINTERVAL", "2000"
          ));
      JsonObject conf = new PostgresClient(Vertx.vertx(), "public").getConnectionConfig();
      PgConnectOptions options =
          PostgresClientInitializer.createPgConnectOptions(conf, PostgresClient.HOST, PostgresClient.PORT);
      assertThat(options.getHost(), is("myhost"));
      assertThat(options.getPort(), is(5433));
      assertThat(options.getUser(), is("myuser"));
      assertThat(options.getPassword(), is("mypassword"));
      assertThat(options.getDatabase(), is("mydatabase"));
      // TODO: enable when available in vertx-sql-client/vertx-pg-client
      // https://issues.folio.org/browse/RMB-657
      // assertThat(options.getConnectionReleaseDelay(), is(1000));
      assertThat(options.getReconnectAttempts(), is(3));
      assertThat(options.getReconnectInterval(), is(2000L));
    } finally {
      // restore defaults
      Envs.setEnv(System.getenv());
    }
  }

  @Test
  public void testPgConnectOptionsWithReaderConfig() throws Exception {
    try {
      Envs.setEnv(Map.of(
          "DB_HOST_READER", "myhost_reader",
          "DB_PORT_READER", "12345",
          "DB_HOST", "myhost",
          "DB_PORT", "5433",
          "DB_USERNAME", "myuser",
          "DB_PASSWORD", "mypassword",
          "DB_DATABASE", "mydatabase",
          "DB_CONNECTIONRELEASEDELAY", "1000",
          "DB_RECONNECTATTEMPTS", "3",
          "DB_RECONNECTINTERVAL", "2000"
      ));
      JsonObject conf = new PostgresClient(Vertx.vertx(), "public").getConnectionConfig();
      PgConnectOptions options =
          PostgresClientInitializer.createPgConnectOptions(conf, PostgresClient.HOST_READER, PostgresClient.PORT_READER);
      assertThat(options.getHost(), is("myhost_reader"));
      assertThat(options.getPort(), is(12345));
      assertThat(options.getUser(), is("myuser"));
      assertThat(options.getPassword(), is("mypassword"));
      assertThat(options.getDatabase(), is("mydatabase"));
      assertThat(options.getReconnectAttempts(), is(3));
      assertThat(options.getReconnectInterval(), is(2000L));
    } finally {
      // restore defaults
      Envs.setEnv(System.getenv());
    }
  }

  @Test
  public void testPgConnectOptionsWithMissingReaderPort() throws Exception {
    try {
      Envs.setEnv(Map.of(
          "DB_HOST_READER", "myhost_reader",
          "DB_HOST", "myhost",
          "DB_PORT", "5433",
          "DB_USERNAME", "myuser",
          "DB_PASSWORD", "mypassword",
          "DB_DATABASE", "mydatabase",
          "DB_CONNECTIONRELEASEDELAY", "1000",
          "DB_RECONNECTATTEMPTS", "3",
          "DB_RECONNECTINTERVAL", "2000"
      ));
      JsonObject conf = new PostgresClient(Vertx.vertx(), "public").getConnectionConfig();
      PgConnectOptions options = PostgresClientInitializer.createPgConnectOptions(conf,
          PostgresClientInitializer.HOST_READER_ASYNC, PostgresClientInitializer.PORT_READER_ASYNC);
      assertNull(options);
    } finally {
      // restore defaults
      Envs.setEnv(System.getenv());
    }
  }
  @Test
  public void testGettingPgPoolWithMissingReaderHost() throws Exception {
    try {
      Envs.setEnv(Map.of(
          "DB_PORT_READER", "12345",
          "DB_HOST", "myhost",
          "DB_PORT", "5433",
          "DB_USERNAME", "myuser",
          "DB_PASSWORD", "mypassword",
          "DB_DATABASE", "mydatabase",
          "DB_CONNECTIONRELEASEDELAY", "1000",
          "DB_RECONNECTATTEMPTS", "3",
          "DB_RECONNECTINTERVAL", "2000"
      ));
      JsonObject conf = new PostgresClient(Vertx.vertx(), "public").getConnectionConfig();
      var initializer = new PostgresClientInitializer(Vertx.vertx(), conf);
      assertNotNull(initializer.getClient());
      assertNotNull(initializer.getReadClient());
      assertNotNull(initializer.getReadClientAsync());
      assertEquals(initializer.getClient(), initializer.getReadClient());
      assertEquals(initializer.getClient(), initializer.getReadClientAsync());
    } finally {
      // restore defaults
      Envs.setEnv(System.getenv());
    }
  }

  @Test
  public void testGettingPgPoolWithNoReader() throws Exception {
    try {
      Envs.setEnv(Map.of(
          "DB_HOST", "myhost",
          "DB_PORT", "5433",
          "DB_USERNAME", "myuser",
          "DB_PASSWORD", "mypassword",
          "DB_DATABASE", "mydatabase",
          "DB_CONNECTIONRELEASEDELAY", "1000",
          "DB_RECONNECTATTEMPTS", "3",
          "DB_RECONNECTINTERVAL", "2000"
      ));
      JsonObject conf = new PostgresClient(Vertx.vertx(), "public").getConnectionConfig();
      var initializer = new PostgresClientInitializer(Vertx.vertx(), conf);
      assertNotNull(initializer.getClient());
      assertNotNull(initializer.getReadClient());
      assertNotNull(initializer.getReadClientAsync());
      assertEquals(initializer.getClient(), initializer.getReadClient());
      assertEquals(initializer.getClient(), initializer.getReadClientAsync());
    } finally {
      // restore defaults
      Envs.setEnv(System.getenv());
    }
  }

  @Test
  public void testGettingPgPoolWithReaderHost() throws Exception {
    try {
      Envs.setEnv(Map.of(
          "DB_PORT_READER", "12345",
          "DB_HOST_READER", "myhost_reader",
          "DB_HOST", "myhost",
          "DB_PORT", "5433",
          "DB_USERNAME", "myuser",
          "DB_PASSWORD", "mypassword",
          "DB_DATABASE", "mydatabase",
          "DB_CONNECTIONRELEASEDELAY", "1000",
          "DB_RECONNECTATTEMPTS", "3",
          "DB_RECONNECTINTERVAL", "2000"
      ));
      JsonObject conf = new PostgresClient(Vertx.vertx(), "public").getConnectionConfig();
      var initializer = new PostgresClientInitializer(Vertx.vertx(), conf);
      assertNotEquals(initializer.getClient(), initializer.getReadClient());
      assertEquals(initializer.getReadClient(), initializer.getReadClientAsync());
    } finally {
      // restore defaults
      Envs.setEnv(System.getenv());
    }
  }

  @Test
  public void testGettingPgPoolWithAsyncReaderHostOnly() throws Exception {
    try {
      Envs.setEnv(Map.of(
          "DB_PORT_READER_ASYNC", "12345",
          "DB_HOST_READER_ASYNC", "myhost_reader",
          "DB_HOST", "myhost",
          "DB_PORT", "5433",
          "DB_USERNAME", "myuser",
          "DB_PASSWORD", "mypassword",
          "DB_DATABASE", "mydatabase",
          "DB_CONNECTIONRELEASEDELAY", "1000",
          "DB_RECONNECTATTEMPTS", "3",
          "DB_RECONNECTINTERVAL", "2000"
      ));
      JsonObject conf = new PostgresClient(Vertx.vertx(), "public").getConnectionConfig();
      var initializer = new PostgresClientInitializer(Vertx.vertx(), conf);
      assertNotNull(initializer.getClient());
      assertNotNull(initializer.getReadClient());
      assertNotNull(initializer.getReadClientAsync());
      assertEquals(initializer.getClient(), initializer.getReadClient());
      assertNotEquals(initializer.getClient(), initializer.getReadClientAsync());
    } finally {
      // restore defaults
      Envs.setEnv(System.getenv());
    }
  }

  @Test
  public void testGettingPgPoolWithAllThreeHostTypes() throws Exception {
    try {
      Envs.setEnv(Map.of(
          "DB_PORT_READER_ASYNC", "12345",
          "DB_HOST_READER_ASYNC", "myhost_reader_async",
          "DB_PORT_READER", "54321",
          "DB_HOST_READER", "myhost_reader",
          "DB_HOST", "myhost",
          "DB_PORT", "5433"
      ));
      JsonObject conf = new PostgresClient(Vertx.vertx(), "public").getConnectionConfig();
      var initializer = new PostgresClientInitializer(Vertx.vertx(), conf);
      assertNotNull(initializer.getClient());
      assertNotNull(initializer.getReadClient());
      assertNotNull(initializer.getReadClientAsync());
      assertNotEquals(initializer.getClient(), initializer.getReadClient());
      assertNotEquals(initializer.getClient(), initializer.getReadClientAsync());
      assertNotEquals(initializer.getReadClient(), initializer.getReadClientAsync());
    } finally {
      // restore defaults
      Envs.setEnv(System.getenv());
    }
  }

  @Test
  public void testGettingPgPoolWithSharedReader() throws Exception {
    try {
      Envs.setEnv(Map.of(
          "DB_PORT_READER", "54321",
          "DB_HOST_READER", "myhost_reader",
          "DB_HOST", "myhost",
          "DB_PORT", "5433"
      ));
      JsonObject conf = new PostgresClient(Vertx.vertx(), "public").getConnectionConfig();
      var initializer = new PostgresClientInitializer(Vertx.vertx(), conf);
      assertNotNull(initializer.getClient());
      assertNotNull(initializer.getReadClient());
      assertNotNull(initializer.getReadClientAsync());
      assertNotEquals(initializer.getClient(), initializer.getReadClient());
      assertNotEquals(initializer.getClient(), initializer.getReadClientAsync());
      assertEquals(initializer.getReadClient(), initializer.getReadClientAsync());
    } finally {
      // restore defaults
      Envs.setEnv(System.getenv());
    }
  }

  @Test
  public void closePostgresTester() {
    PostgresTester postgresTester1 = mock(PostgresTester.class);
    PostgresTester postgresTester2 = mock(PostgresTester.class);
    PostgresClient.setPostgresTester(postgresTester1);
    PostgresClient.setPostgresTester(postgresTester1);
    verify(postgresTester1, times(1)).close();
    PostgresClient.setPostgresTester(postgresTester2);
    verify(postgresTester1, times(2)).close();
    PostgresClient.stopPostgresTester();
    verify(postgresTester1, times(2)).close();
    verify(postgresTester2, times(1)).close();
  }

  @Test
  public void testProcessResults() {
    PostgresClient testClient = PostgresClient.testClient();

    int total = 15;
    RowSet<Row> rs = getMockTestPojoResultSet(total);

    List<TestPojo> results = testClient.processResults(rs, total, DEFAULT_OFFSET, DEFAULT_LIMIT, TestPojo.class).getResults();

    assertTestPojoResults(results, total);
  }

  @Test
  public void testDeserializeResults() {
    PostgresClient testClient = PostgresClient.testClient();

    int total = 25;
    RowSet<Row> rs = getMockTestPojoResultSet(total);
    PostgresClient.ResultsHelper<TestPojo> resultsHelper = new PostgresClient.ResultsHelper<>(rs, total, TestPojo.class);

    testClient.deserializeResults(resultsHelper);

    assertTestPojoResults(resultsHelper.list, total);
  }

  @Test
  public void testIsAuditFlavored() {
    PostgresClient testClient = PostgresClient.testClient();
    assertThat(testClient.isAuditFlavored(TestJsonbPojo.class), is(true));
    assertThat(testClient.isAuditFlavored(TestPojo.class), is(false));
  }

  @Test
  public void testGetExternalColumnSetters() throws NoSuchMethodException {
    PostgresClient testClient = PostgresClient.testClient();
    List<String> columnNames = new ArrayList<String>(Arrays.asList(new String[] {
      "foo", "bar", "biz", "baz"
    }));
    Map<String, Method> externalColumnSetters = new HashMap<>();
    testClient.collectExternalColumnSetters(columnNames, TestPojo.class, false, externalColumnSetters);
    assertThat(externalColumnSetters.size(), is(4));
    assertThat(externalColumnSetters.get("foo"), is(TestPojo.class.getMethod(testClient.databaseFieldToPojoSetter("foo"), String.class)));
    assertThat(externalColumnSetters.get("bar"), is(TestPojo.class.getMethod(testClient.databaseFieldToPojoSetter("bar"), String.class)));
    assertThat(externalColumnSetters.get("biz"), is(TestPojo.class.getMethod(testClient.databaseFieldToPojoSetter("biz"), Double.class)));
    assertThat(externalColumnSetters.get("baz"), is(TestPojo.class.getMethod(testClient.databaseFieldToPojoSetter("baz"), List.class)));
  }

  @Test
  public void testPopulateExternalColumns() throws Exception {
    PostgresClient testClient = PostgresClient.testClient();
    List<String> columnNames = new ArrayList<String>(Arrays.asList(new String[] {
      "id", "foo", "bar", "biz", "baz"
    }));
    Map<String, Method> externalColumnSetters = new HashMap<>();
    testClient.collectExternalColumnSetters(columnNames, TestPojo.class, false, externalColumnSetters);
    externalColumnSetters.put("nonExistingColumn", TestPojo.class.getMethod("setBar", String.class));
    TestPojo o = new TestPojo();
    String foo = "Hello";
    String bar = "World";
    Double biz = 1.0;
    String [] baz = new String[] { "This", "is", "a", "test" };

    RowDesc desc = new LocalRowDesc(List.of("foo", "bar", "biz", "baz"));
    Row row = new RowImpl(desc);
    row.addString(foo);
    row.addString(bar);
    row.addDouble(biz);
    row.addArrayOfString(baz);

    testClient.populateExternalColumns(externalColumnSetters, o, row);
    assertThat(o.getFoo(), is(foo));
    assertThat(o.getBar(), is(bar));
    assertThat(o.getBiz(), is(biz));
    assertThat(o.getBaz().size(), is(baz.length));
    assertThat(o.getBaz().get(0), is(baz[0]));
    assertThat(o.getBaz().get(1), is(baz[1]));
    assertThat(o.getBaz().get(2), is(baz[2]));
    assertThat(o.getBaz().get(3), is(baz[3]));
  }

  @Test
  public void testDatabaseFieldToPojoSetter() {
    PostgresClient testClient = PostgresClient.testClient();
    String setterMethodName = testClient.databaseFieldToPojoSetter("test_field");
    assertThat(setterMethodName, is("setTestField"));
  }

  public class FakeSqlConnection extends PgConnectionMock {
    final AsyncResult<RowSet<Row>> asyncResult;
    final boolean failExplain;

    FakeSqlConnection(AsyncResult<RowSet<Row>> result, boolean failExplain) {
      this.asyncResult = result;
      this.failExplain = failExplain;
    }

    @Override
    public Query<RowSet<Row>> query(String s) {

      return new Query<RowSet<Row>>() {

        @Override
        public void execute(Handler<AsyncResult<RowSet<Row>>> handler) {
          if (s.startsWith("EXPLAIN") && failExplain) {
            handler.handle(Future.failedFuture("failExplain"));
          } else if (s.startsWith("COUNT ") && asyncResult.succeeded()) {
            List<String> columnNames = List.of("COUNT");
            RowDesc rowDesc = new LocalRowDesc(columnNames);
            Row row = new RowImpl(rowDesc);
            row.addInteger(asyncResult.result().size());
            List<Row> rows = new LinkedList<>();
            rows.add(row);
            RowSet rowSet = new LocalRowSet(asyncResult.result().size()).withColumns(columnNames).withRows(rows);
            handler.handle(Future.succeededFuture(rowSet));
          } else {
            handler.handle(asyncResult);
          }
        }

        @Override
        public Future<RowSet<Row>> execute() {
          return Future.future(promise -> execute(promise));
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
  }

  @Test
  public void testProcessQueryWithCount()  {
    PostgresClient testClient = PostgresClient.testClient();
    QueryHelper queryHelper = new QueryHelper("test_pojo");
    queryHelper.selectQuery = "SELECT * FROM test_pojo";
    queryHelper.countQuery = "COUNT (*) FROM test_pojo";

    int total = 10;

    PgConnection connection = new FakeSqlConnection(Future.succeededFuture(getMockTestJsonbPojoResultSet(total)), false);

    testClient.processQueryWithCount(connection, queryHelper, "get",
      totaledResults -> {
        assertThat(totaledResults.estimatedTotal, is(total));
        return testClient.processResults(totaledResults.set, totaledResults.estimatedTotal, DEFAULT_OFFSET, DEFAULT_LIMIT, TestPojo.class);
      }).onSuccess(reply -> assertTestPojoResults(reply.getResults(), total));
  }

  @Test
  public void testProcessQuery() {
    PostgresClient testClient = PostgresClient.testClient();
    List<FacetField> facets = new ArrayList<FacetField>() {
      {
        add(new FacetField("jsonb->>'biz'"));
      }
    };
    QueryHelper queryHelper = new QueryHelper("test_jsonb_pojo");
    queryHelper.selectQuery = "SELECT id, foo, bar FROM test_jsonb_pojo LIMIT 30 OFFSET 1";

    int total = 30;

    PgConnection connection = new FakeSqlConnection(Future.succeededFuture(getMockTestJsonbPojoResultSet(total)), true);

    testClient.processQuery(connection, queryHelper, total, "get",
      totaledResults -> testClient.processResults(totaledResults.set, totaledResults.estimatedTotal, DEFAULT_OFFSET, DEFAULT_LIMIT, TestJsonbPojo.class),
      reply -> {
        List<TestJsonbPojo> results = reply.result().getResults();

        assertTestJsonbPojoResults(results, total);
      }
    );

  }

  @Test
  public void testProcessQueryFails() {
    PostgresClient testClient = PostgresClient.testClient();
    QueryHelper queryHelper = new QueryHelper("test_jsonb_pojo");
    queryHelper.selectQuery = "SELECT foo";

    PgConnection connection = new FakeSqlConnection(Future.failedFuture("Bad query"), false);

    testClient.processQuery(connection, queryHelper, 30, "get",
      totaledResults -> testClient.processResults(totaledResults.set, totaledResults.estimatedTotal, DEFAULT_OFFSET, DEFAULT_LIMIT, TestJsonbPojo.class),
      reply -> {
        assertThat(reply.failed(), is(true));
        assertThat(reply.cause().getMessage(), is("Bad query"));
      }
    );
  }

  @Test
  public void testProcessQueryException() {
    PostgresClient testClient = PostgresClient.testClient();
    QueryHelper queryHelper = new QueryHelper("test_jsonb_pojo");
    queryHelper.selectQuery = "SELECT foo";

    PgConnection connection = null;
    testClient.processQuery(connection, queryHelper, 30, "get",
      totaledResults -> testClient.processResults(totaledResults.set, totaledResults.estimatedTotal, DEFAULT_OFFSET, DEFAULT_LIMIT, TestJsonbPojo.class),
      reply -> {
        assertThat(reply.failed(), is(true));
        assertThat(reply.cause() instanceof NullPointerException, is(true));
      }
    );
  }

  private RowSet<Row> getMockTestPojoResultSet(int total) {
    List<String> columnNames = List.of("id", "foo", "bar", "biz", "baz");
    RowDesc rowDesc = new LocalRowDesc(columnNames);
    List<Row> rows = new LinkedList<>();
    for (int i = 0; i < total; i++) {
      Row row = new RowImpl(rowDesc);
      row.addUUID(UUID.randomUUID());
      row.addString("foo " + i);
      row.addString("bar " + i);
      row.addDouble((double) i);
      row.addArrayOfString(new String[] { "This", "is", "a", "test" } );
      rows.add(row);
    }
    return new LocalRowSet(total).withColumns(columnNames).withRows(rows);
  }

  private void assertTestPojoResults(List<TestPojo> results, int total) {
    assertThat(results.size(), is(total));

    for(int i = 0; i < total; i++) {
      assertThat(results.get(i).getFoo(), is("foo " + i));
      assertThat(results.get(i).getBar(), is("bar " + i));
      assertThat(results.get(i).getBiz(), is((double)i));
      assertThat(results.get(i).getBaz().size(), is(4));
      assertThat(results.get(i).getBaz().get(0), is("This"));
      assertThat(results.get(i).getBaz().get(1), is("is"));
      assertThat(results.get(i).getBaz().get(2), is("a"));
      assertThat(results.get(i).getBaz().get(3), is("test"));
    }
  }

  private RowSet<Row> getMockTestJsonbPojoResultSet(int total) {
    List<String> columnNames = List.of("jsonb");
    RowDesc rowDesc = new LocalRowDesc(columnNames);
    List<String> baz = new ArrayList<String>(Arrays.asList(new String[] {
        "This", "is", "a", "test"
    }));
    List<Row> rows = new LinkedList<>();
    for (int i = 0; i < total; i++) {
      Row row = new RowImpl(rowDesc);
      row.addValue(new JsonObject()
          .put("id", UUID.randomUUID().toString())
          .put("foo", "foo " + i)
          .put("bar", "bar " + i)
          .put("biz", (double) i)
          .put("baz", baz)
      );
      rows.add(row);
    }
    return new LocalRowSet(total).withColumns(columnNames).withRows(rows);
  }

  private void assertTestJsonbPojoResults(List<TestJsonbPojo> results, int total) {
    assertThat(results.size(), is(total));

    for(int i = 0; i < total; i++) {
      assertThat(results.get(i).getJsonb().getString("foo"), is("foo " + i));
      assertThat(results.get(i).getJsonb().getString("bar"), is("bar " + i));
      assertThat(results.get(i).getJsonb().getDouble("biz"), is((double)i));
      assertThat(results.get(i).getJsonb().getJsonArray("baz").size(), is(4));
      assertThat(results.get(i).getJsonb().getJsonArray("baz").getString(0), is("This"));
      assertThat(results.get(i).getJsonb().getJsonArray("baz").getString(1), is("is"));
      assertThat(results.get(i).getJsonb().getJsonArray("baz").getString(2), is("a"));
      assertThat(results.get(i).getJsonb().getJsonArray("baz").getString(3), is("test"));
    }
  }

  static class TestPojo {
    private String id;
    private String foo;
    private String bar;
    private Double biz;
    private List<String> baz;
    public TestPojo() {

    }
    public TestPojo(String id, String foo, String bar, Double biz) {
      this.id = id;
      this.foo = foo;
      this.bar = bar;
      this.biz = biz;
    }
    public TestPojo(String id, String foo, String bar, Double biz, List<String> baz) {
      this(id, foo, bar, biz);
      this.baz = baz;
    }
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getFoo() {
      return foo;
    }
    public void setFoo(String foo) {
      this.foo = foo;
    }
    public String getBar() {
      return bar;
    }
    public void setBar(String bar) {
      this.bar = bar;
    }
    public Double getBiz() {
      return biz;
    }
    public void setBiz(Double biz) {
      this.biz = biz;
    }
    public List<String> getBaz() {
      return baz;
    }
    public void setBaz(List<String> baz) {
      this.baz = baz;
    }
  }

  static class TestJsonbPojo {
    private JsonObject jsonb;
    public TestJsonbPojo() {

    }
    public TestJsonbPojo(JsonObject jsonb) {
      this.jsonb = jsonb;
    }
    public JsonObject getJsonb() {
      return jsonb;
    }
    public void setJsonb(JsonObject jsonb) {
      this.jsonb = jsonb;
    }
  }

  @Test
  public void splitSqlStatementsSingleLine() {
    assertThat(PostgresClient.splitSqlStatements("foo bar"),
        is(arrayContaining("foo bar")));
  }

  @Test
  public void splitSqlStatementsLines() {
    assertThat(PostgresClient.splitSqlStatements("foo\nbar\rbaz\r\nend"),
        is(arrayContaining("foo", "bar", "baz", "end")));
  }

  @Test
  public void splitSqlStatementsDollarQuoting() {
    assertThat(PostgresClient.splitSqlStatements("foo\nbar $$\rbaz\r\n$$ end"),
        is(arrayContaining("foo", "bar $$\rbaz\r\n$$ end", "")));
  }

  @Test
  public void splitSqlStatementsNestedDollarQuoting() {
    assertThat(PostgresClient.splitSqlStatements(
        "DO $xyz$ SELECT\n$xyzabc$Rock 'n' Roll$xyzabc$;\n$xyz$;\r\nSELECT $$12$xyz$34$xyz$56$$;\nSELECT $$12$$;"),
        is(arrayContaining(
            "",
            "DO $xyz$ SELECT\n$xyzabc$Rock 'n' Roll$xyzabc$;\n$xyz$;",
            "SELECT $$12$xyz$34$xyz$56$$;",
            "SELECT $$12$$;",
            "")));
  }

  @Test
  public void preprocessSqlStatements() throws Exception {
    String sqlFile = ResourceUtil.asString("import.sql");
    assertThat(PostgresClient.preprocessSqlStatements(sqlFile), hasItemInArray(stringContainsInOrder(
        "COPY test.po_line", "24\t")));
  }

  @Test
  public void pojo2JsonObject() throws Exception {
    String id = UUID.randomUUID().toString();
    User user = new User().withId(id).withUsername("name").withVersion(5);
    JsonObject json = PostgresClient.pojo2JsonObject(user);
    assertThat(json.getMap(), is(Map.of("id", id, "username", "name", "_version", 5)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void pojo2JsonObjectNull() throws Exception {
    PostgresClient.pojo2JsonObject(null);
  }

  @Test
  public void pojo2JsonObjectJson() throws Exception {
    JsonObject j = new JsonObject().put("a", "b");
    Assert.assertEquals(j.encode(), PostgresClient.pojo2JsonObject(j).encode());
  }

  @Test
  public void pojo2JsonObjectMap() throws Exception {
    Map<String,String> m = new HashMap<>();
    m.put("a", "b");
    Assert.assertEquals("{\"a\":\"b\"}", PostgresClient.pojo2JsonObject(m).encode());
  }

  @Test
  public void pojo2JsonObjectMap2() throws Exception {
    UUID id = UUID.randomUUID();
    Map<UUID,String> m = new HashMap<>();
    m.put(id, "b");
    Assert.assertEquals("{\"" + id.toString() + "\":\"b\"}", PostgresClient.pojo2JsonObject(m).encode());
  }

  @Test(expected = Exception.class)
  public void pojo2JsonObjectBadMap() throws Exception {
    PostgresClient.pojo2JsonObject(this);
  }

  @Test
  public void getTotalRecordsTest() {
    assertNull(PostgresClient.getTotalRecords(10, null, 0, 0));

    assertEquals((Integer)20, PostgresClient.getTotalRecords(10, 20, 0, 0));

    assertEquals((Integer)20, PostgresClient.getTotalRecords(10, 20, 0, 10));

    assertEquals((Integer)10, PostgresClient.getTotalRecords(0, 20, 10, 20));

    assertEquals((Integer)20, PostgresClient.getTotalRecords(10, 30, 10, 20));

    assertEquals((Integer)30, PostgresClient.getTotalRecords(10, 20, 20, 10));

    assertEquals((Integer) 25, PostgresClient.getTotalRecords(5, 20, 20, 10));
  }

  @Test
  public void getModuleName() {
    Exception e = assertThrows(RuntimeException.class, () -> PostgresClient.getModuleName("foo.Bar"));
    assertThat(e.getMessage(), containsString("src/test/java/foo/Bar.java"));
    assertThat(e.getCause(), is(instanceOf(ClassNotFoundException.class)));
  }

  @Test
  public void postgresClientAES() throws Exception {
    AES.setSecretKey(null);
    assertThat(PostgresClient.createPassword(AESTest.PASSWORD), is(AESTest.PASSWORD));
    assertThat(PostgresClient.decodePassword(AESTest.ENCRYPTED_PASSWORD_BASE64), is(AESTest.ENCRYPTED_PASSWORD_BASE64));
    AES.setSecretKey(AESTest.SECRET_KEY);
    assertThat(PostgresClient.createPassword(AESTest.PASSWORD), is(AESTest.ENCRYPTED_PASSWORD_BASE64));
    assertThat(PostgresClient.decodePassword(AESTest.ENCRYPTED_PASSWORD_BASE64), is(AESTest.PASSWORD));
    AES.setSecretKey(null);
  }
}
