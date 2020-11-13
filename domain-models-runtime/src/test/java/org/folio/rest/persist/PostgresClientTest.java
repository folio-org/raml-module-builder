package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.ArrayMatching.arrayContaining;
import static org.hamcrest.collection.ArrayMatching.hasItemInArray;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.text.StringContainsInOrder.stringContainsInOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
import io.vertx.core.logging.Logger;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgNotification;
import io.vertx.pgclient.impl.RowImpl;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.impl.RowDesc;
import io.vertx.sqlclient.spi.DatabaseMetadata;
import org.folio.rest.persist.facets.FacetField;
import org.folio.rest.persist.helpers.LocalRowSet;
import org.folio.rest.tools.utils.Envs;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.util.ResourceUtil;
import org.folio.rest.persist.PostgresClient.QueryHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class PostgresClientTest {
  // See PostgresClientIT.java for the tests that require a postgres database!

  private Logger oldLogger;
  private String oldConfigFilePath;
  private boolean oldIsEmbedded;
  private int oldEmbeddedPort;
  /** empty = no environment variables */
  private JsonObject empty = new JsonObject();

  private static final int DEFAULT_OFFSET = 0;

  private static final int DEFAULT_LIMIT = 10;

  @Before
  public void initConfig() {
    oldConfigFilePath = PostgresClient.getConfigFilePath();
    PostgresClient.setConfigFilePath(null);
    oldIsEmbedded = PostgresClient.isEmbedded();
    PostgresClient.setIsEmbedded(false);
    oldEmbeddedPort = PostgresClient.getEmbeddedPort();
    PostgresClient.setEmbeddedPort(-1);
  }

  @After
  public void restoreConfig() {
    PostgresClient.setConfigFilePath(oldConfigFilePath);
    PostgresClient.setIsEmbedded(oldIsEmbedded);
    PostgresClient.setEmbeddedPort(oldEmbeddedPort);
  }

  @Test
  public void configDefault() throws Exception {
    assertThat("Port 6000 must be free for embedded postgres", NetworkUtils.isLocalPortFree(6000), is(true));
    PostgresClient.setConfigFilePath("nonexisting");
    JsonObject config = PostgresClient.getPostgreSQLClientConfig(/* default schema = */ "public", null, empty);
    assertThat("embedded postgres", PostgresClient.isEmbedded(), is(true));
    assertThat(config.getString("host"), is("127.0.0.1"));
    assertThat(config.getInteger("port"), is(6000));
    assertThat(config.getString("username"), is("username"));
  }

  @Test
  public void configDefaultWithPortAndTenant() throws Exception {
    int port = NetworkUtils.nextFreePort();
    PostgresClient.setConfigFilePath("nonexisting");
    PostgresClient.setEmbeddedPort(port);
    JsonObject config = PostgresClient.getPostgreSQLClientConfig("footenant", "barschema", empty);
    assertThat("embedded postgres", PostgresClient.isEmbedded(), is(true));
    assertThat(config.getString("host"), is("127.0.0.1"));
    assertThat(config.getInteger("port"), is(port));
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
    JsonObject config = PostgresClient.getPostgreSQLClientConfig("footenant", "aSchemaName", env);
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
    JsonObject config = PostgresClient.getPostgreSQLClientConfig("public", null, empty);
    assertThat(config.getString("host"), is("localhost"));
    assertThat(config.getInteger("port"), is(5433));
    assertThat(config.getString("username"), is("postgres"));
    assertThat(config.getInteger("connectionReleaseDelay"), is(30000));
  }

  @Test
  public void configFileTenant() throws Exception {
    // values from src/test/resources/my-postgres-conf.json
    PostgresClient.setConfigFilePath("/my-postgres-conf.json");
    JsonObject config = PostgresClient.getPostgreSQLClientConfig("footenant", "mySchemaName", empty);
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
    PgConnectOptions options = PostgresClient.createPgConnectOptions(conf);
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
  public void testPgConnectOptionsFull() {
    JsonObject conf = new JsonObject()
        .put("host", "myhost")
        .put("port", 5433)
        .put("username", "myuser")
        .put("password", "mypassword")
        .put("database", "mydatabase")
        .put("connectionReleaseDelay", 1000);

    PgConnectOptions options = PostgresClient.createPgConnectOptions(conf);
    assertThat("myhost", is(options.getHost()));
    assertThat(5433, is(options.getPort()));
    assertThat("myuser", is(options.getUser()));
    assertThat("mypassword", is(options.getPassword()));
    assertThat("mydatabase", is(options.getDatabase()));
    // TODO: enable when available in vertx-sql-client/vertx-pg-client
    // https://issues.folio.org/browse/RMB-657
    // assertThat(1000, is(options.getConnectionReleaseDelay()));
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

    List<String> rowColumns = new LinkedList<>();
    rowColumns.add("foo");
    rowColumns.add("bar");
    rowColumns.add("biz");
    rowColumns.add("baz");
    RowDesc desc = new RowDesc(rowColumns);
    Row row = new RowImpl(desc);
    row.addString(foo);
    row.addString(bar);
    row.addDouble(biz);
    row.addStringArray(baz);

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

  public class FakeSqlConnection implements PgConnection {
    final AsyncResult<RowSet<Row>> asyncResult;
    final boolean failExplain;

    FakeSqlConnection(AsyncResult<RowSet<Row>> result, boolean failExplain) {
      this.asyncResult = result;
      this.failExplain = failExplain;
    }

    @Override
    public PgConnection notificationHandler(Handler<PgNotification> handler) {
      return this;
    }

    @Override
    public PgConnection cancelRequest(Handler<AsyncResult<Void>> handler) {
      handler.handle(Future.failedFuture("not implemented"));
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
      handler.handle(Future.failedFuture("not implemented"));
      return this;
    }

    @Override
    public PgConnection exceptionHandler(Handler<Throwable> handler) {
      return this;
    }

    @Override
    public PgConnection closeHandler(Handler<Void> handler) {
      return null;
    }

    @Override
    public Transaction begin() {
      return null;
    }

    @Override
    public boolean isSSL() {
      return false;
    }

    @Override
    public Query<RowSet<Row>> query(String s) {

      return new Query<RowSet<Row>>() {

        @Override
        public void execute(Handler<AsyncResult<RowSet<Row>>> handler) {
          if (s.startsWith("EXPLAIN") && failExplain) {
            handler.handle(Future.failedFuture("failExplain"));
          } else if (s.startsWith("COUNT ") && asyncResult.succeeded()) {
            List<String> columnNames = new LinkedList<>();
            columnNames.add("COUNT");
            RowDesc rowDesc = new RowDesc(columnNames);
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
      return null;
    }

    @Override
    public void close() {
    }

    @Override
    public DatabaseMetadata databaseMetadata() {
      return null;
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
      },
      reply -> {
        List<TestPojo> results = reply.result().getResults();

        assertTestPojoResults(results, total);
      }
    );

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
    List<String> columnNames = new ArrayList<String>(Arrays.asList(new String[] {
      "id", "foo", "bar", "biz", "baz"
    }));
    RowDesc rowDesc = new RowDesc(columnNames);
    List<Row> rows = new LinkedList<>();
    for (int i = 0; i < total; i++) {
      Row row = new RowImpl(rowDesc);
      row.addUUID(UUID.randomUUID());
      row.addString("foo " + i);
      row.addString("bar " + i);
      row.addDouble((double) i);
      row.addStringArray(new String[] { "This", "is", "a", "test" } );
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
    List<String> columnNames = new ArrayList<String>(Arrays.asList(new String[] {
      "jsonb"
    }));
    RowDesc rowDesc = new RowDesc(columnNames);
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

  @Before
  public void saveLogger() {
    oldLogger = PostgresClient.log;
  }

  @After
  public void restoreLogger() {
    PostgresClient.log = oldLogger;
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

}
