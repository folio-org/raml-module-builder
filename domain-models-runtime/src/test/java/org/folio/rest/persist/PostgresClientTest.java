package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.folio.rest.persist.facets.FacetField;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.persist.PostgresClient.QueryHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.ext.asyncsql.impl.PostgreSQLConnectionImpl;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLOperations;

import freemarker.template.TemplateException;

public class PostgresClientTest {
  // See PostgresClientIT.java for the tests that require a postgres database!

  private Logger oldLogger;
  private String oldConfigFilePath;
  private boolean oldIsEmbedded;
  private int oldEmbeddedPort;
  /** empty = no environment variables */
  private JsonObject empty = new JsonObject();

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
    assertThat(config.getInteger("connectionReleaseDelay"), is(60000));
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
    assertThat(config.getInteger("connectionReleaseDelay"), is(60000));
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
  public void testProcessResults() {
    PostgresClient testClient = PostgresClient.testClient();

    int total = 15;
    ResultSet rs = getMockTestPojoResultSet(total);

    List<TestPojo> results = testClient.processResults(rs, total, TestPojo.class).getResults();

    assertTestPojoResults(results, total);
  }

  @Test
  public void testDeserializeResults() {
    PostgresClient testClient = PostgresClient.testClient();

    int total = 25;
    ResultSet rs = getMockTestPojoResultSet(total);
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
    Map<String, Method> externalColumnSettters = testClient.getExternalColumnSetters(columnNames, TestPojo.class, false);
    assertThat(externalColumnSettters.size(), is(4));
    assertThat(externalColumnSettters.get("foo"), is(TestPojo.class.getMethod(testClient.databaseFieldToPojoSetter("foo"), String.class)));
    assertThat(externalColumnSettters.get("bar"), is(TestPojo.class.getMethod(testClient.databaseFieldToPojoSetter("bar"), String.class)));
    assertThat(externalColumnSettters.get("biz"), is(TestPojo.class.getMethod(testClient.databaseFieldToPojoSetter("biz"), Double.class)));
    assertThat(externalColumnSettters.get("baz"), is(TestPojo.class.getMethod(testClient.databaseFieldToPojoSetter("baz"), List.class)));
  }

  @Test
  public void testPopulateExternalColumns() {
    PostgresClient testClient = PostgresClient.testClient();
    List<String> columnNames = new ArrayList<String>(Arrays.asList(new String[] {
      "id", "foo", "bar", "biz", "baz"
    }));
    Map<String, Method> externalColumnSettters = testClient.getExternalColumnSetters(columnNames, TestPojo.class, false);
    TestPojo o = new TestPojo();
    String foo = "Hello";
    String bar = "World";
    Double biz = 1.0;
    List<String> baz = new ArrayList<String>(Arrays.asList(new String[] {
      "This", "is", "a", "test"
    }));
    JsonObject row = new JsonObject()
        .put("foo", foo)
        .put("bar", bar)
        .put("biz", biz)
        .put("baz", baz);
    testClient.populateExternalColumns(externalColumnSettters, o, row);
    assertThat(o.getFoo(), is(foo));
    assertThat(o.getBar(), is(bar));
    assertThat(o.getBiz(), is(biz));
    assertThat(o.getBaz().size(), is(baz.size()));
    assertThat(o.getBaz().get(0), is(baz.get(0)));
    assertThat(o.getBaz().get(1), is(baz.get(1)));
    assertThat(o.getBaz().get(2), is(baz.get(2)));
    assertThat(o.getBaz().get(3), is(baz.get(3)));
  }

  @Test
  public void testDatabaseFieldToPojoSetter() {
    PostgresClient testClient = PostgresClient.testClient();
    String setterMethodName = testClient.databaseFieldToPojoSetter("test_field");
    assertThat(setterMethodName, is("setTestField"));
  }

  @Test
  public void testProcessQueryWithCount() throws IOException, TemplateException {
    PostgresClient testClient = PostgresClient.testClient();
    QueryHelper queryHelper = new QueryHelper("test_pojo");
    queryHelper.selectQuery = "SELECT * FROM test_pojo";
    queryHelper.countQuery = "COUNT (*) FROM test_pojo";

    int total = 10;

    SQLConnection connection = new PostgreSQLConnectionImpl(null, null, null) {
      @Override
      public SQLOperations querySingle(String sql, Handler<AsyncResult<JsonArray>> handler) {
        handler.handle(Future.succeededFuture(new JsonArray().add(total)));
        return null;
      }
      @Override
      public SQLConnection query(String sql, Handler<AsyncResult<ResultSet>> handler) {
        ResultSet rs = getMockTestPojoResultSet(total);
        handler.handle(Future.succeededFuture(rs));
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

    testClient.processQueryWithCount(connection, queryHelper, "get",
      totaledResults -> {
        assertThat(totaledResults.total, is(total));
        return testClient.processResults(totaledResults.set, totaledResults.total, TestPojo.class);
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

    SQLConnection connection = new PostgreSQLConnectionImpl(null, null, null) {
      @Override
      public SQLConnection query(String sql, Handler<AsyncResult<ResultSet>> handler) {
        // provoke explain query failure
        if (sql.startsWith("EXPLAIN ")) {
          handler.handle(Future.failedFuture("explain"));
          return this;
        }
        ResultSet rs = getMockTestJsonbPojoResultSet(total);
        handler.handle(Future.succeededFuture(rs));
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

    testClient.processQuery(connection, queryHelper, total, "get",
      totaledResults -> testClient.processResults(totaledResults.set, totaledResults.total, TestJsonbPojo.class),
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

    SQLConnection connection = new PostgreSQLConnectionImpl(null, null, null) {
      @Override
      public SQLConnection query(String sql, Handler<AsyncResult<ResultSet>> handler) {
        handler.handle(Future.failedFuture("Bad query"));
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

    testClient.processQuery(connection, queryHelper, 30, "get",
      totaledResults -> testClient.processResults(totaledResults.set, totaledResults.total, TestJsonbPojo.class),
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

    SQLConnection connection = null;
    testClient.processQuery(connection, queryHelper, 30, "get",
      totaledResults -> testClient.processResults(totaledResults.set, totaledResults.total, TestJsonbPojo.class),
      reply -> {
        assertThat(reply.failed(), is(true));
        assertThat(reply.cause() instanceof NullPointerException, is(true));
      }
    );
  }

  private ResultSet getMockTestPojoResultSet(int total) {
    List<String> columnNames = new ArrayList<String>(Arrays.asList(new String[] {
      "id", "foo", "bar", "biz", "baz"
    }));

    List<String> baz = new ArrayList<String>(Arrays.asList(new String[] {
      "This", "is", "a", "test"
    }));

    List<JsonArray> list = new ArrayList<JsonArray>();

    for(int i = 0; i < total; i++) {
      list.add(new JsonArray()
        .add(UUID.randomUUID().toString())
        .add("foo " + i)
        .add("bar " + i)
        .add((double) i)
        .add(baz)
      );
    }

    return new ResultSet(columnNames, list, null);
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

  private ResultSet getMockTestJsonbPojoResultSet(int total) {
    List<String> columnNames = new ArrayList<String>(Arrays.asList(new String[] {
      "jsonb"
    }));

    List<String> baz = new ArrayList<String>(Arrays.asList(new String[] {
      "This", "is", "a", "test"
    }));

    List<JsonArray> list = new ArrayList<JsonArray>();

    for(int i = 0; i < total; i++) {
      list.add(new JsonArray()
        .add(new JsonObject()
          .put("id", UUID.randomUUID().toString())
          .put("foo", "foo " + i)
          .put("bar", "bar " + i)
          .put("biz", (double) i)
          .put("baz", baz)
        )
      );
    }

    return new ResultSet(columnNames, list, null);
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
}
