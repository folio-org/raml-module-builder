package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.folio.rest.persist.facets.FacetField;
import org.folio.rest.persist.facets.ParsedQuery;
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
import io.vertx.ext.asyncsql.impl.PostgreSQLConnectionImpl;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLOperations;

import freemarker.template.TemplateException;

import net.sf.jsqlparser.JSQLParserException;

public class PostgresClientTest {
  // See PostgresClientIT.java for the tests that require a postgres database!

  private static final String notTrue = "AND \\\\(\\\\(\\\\(FALSE\\\\)\\\\)\\\\)";
  private static final String isTrue = "AND \\\\(\\\\(\\\\(TRUE\\\\)\\\\)\\\\)";

  String []queries = new String[]{
  //"SELECT * FROM table WHERE items_mt_view.jsonb->>' ORDER BY items_mt_view.jsonb->>\\'aaa\\'  ORDER BY items2_mt_view.jsonb' ORDER BY items_mt_view.jsonb->>'aaa limit' OFFSET 31 limit 10",
  "SELECT * FROM table WHERE items_mt_view.jsonb->>'title' LIKE '%12345%' ORDER BY items_mt_view.jsonb->>'title' DESC OFFSET 30 limit 10",
  "select jsonb FROM counter_mod_inventory_storage.item  WHERE jsonb@>'{\"barcode\":4}' order by jsonb->'a'  asc",
  "select jsonb FROM counter_mod_inventory_storage.item  WHERE jsonb @> '{\"barcode\":4}' limit 100 offset 0",
  "select jsonb FROM counter_mod_inventory_storage.item  WHERE jsonb @> '{\" AND IS TRUE \":4}' limit 100 offset 0",
  //"SELECT * FROM table WHERE items0_mt_view.jsonb->>' ORDER BY items1_mt_view.jsonb->>''aaa'' ' ORDER BY items2_mt_view.jsonb->>' ORDER BY items3_mt_view.jsonb->>''aaa'' '",
  "SELECT _id FROM test_tenant_mod_inventory_storage.material_type  WHERE jsonb@>'{\"id\":\"af6c5503-71e7-4b1f-9810-5c9f1af7c570\"}' LIMIT 1 OFFSET 0 ",
  "select * from diku999_circulation_storage.audit_loan WHERE audit_loan.jsonb->>'id' = 'cf23adf0-61ba-4887-bf82-956c4aae2260 order by created_date LIMIT 10 OFFSET 0' order by created_date LIMIT 10 OFFSET 0 ",
  "select * from slowtest99_mod_inventory_storage.item where (item.jsonb->'barcode') = to_jsonb('1000000'::int)  order by a LIMIT 30;",
  "SELECT  * FROM slowtest_cql5_mod_inventory_storage.item  WHERE lower(f_unaccent(item.jsonb->>'default')) LIKE lower(f_unaccent('true')) ORDER BY lower(f_unaccent(item.jsonb->>'code')) DESC LIMIT 10 OFFSET 0",
  //"SELECT * FROM harvard_mod_configuration.config_data  WHERE ((true) AND ( (config_data.jsonb->>'userId' ~ '') IS NOT TRUE)) ORDER BY lower(f_unaccent(item.jsonb->>'code')) DESC, item.jsonb->>'code' DESC LIMIT 10 OFFSET 0",
  //"SELECT * FROM harvard_mod_configuration.config_data  WHERE ((true) AND ( (config_data.jsonb->>'userId' ~ '') IS TRUE)) OR (lower(f_unaccent(config_data.jsonb->>'userId')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]])joeshmoe($|[[:punct:]]|[[:space:]])')))  ORDER BY lower(f_unaccent(item.jsonb->>'code')) DESC, item.jsonb->>'code' DESC LIMIT 10 OFFSET 0",
  "SELECT * FROM t WHERE TRUE AND lower(f_unaccent(item.jsonb->>'default')) IS NOT NULL ORDER BY lower(f_unaccent(item.jsonb->>'code')) DESC",
  "SELECT * FROM t WHERE TRUE AND lower(f_unaccent(item.jsonb->>'default')) IS NOT TRUE ORDER BY lower(f_unaccent(item.jsonb->>'code')) DESC",
  "SELECT * FROM harvard5_mod_inventory_storage.material_type  where (jsonb->>'test'  ~ '') IS NOT TRUE limit 10",
  "SELECT * FROM harvard5_mod_inventory_storage.material_type  where (jsonb->>'test'  ~ '') IS TRUE limit 10",
  "SELECT * FROM harvard5_mod_inventory_storage.material_type  where (jsonb->>'test'  ~ '') IS TRUE AND (jsonb->>'test'  ~ '') IS NOT TRUE limit 10",
  "SELECT * FROM t WHERE ((((true) AND ( (instance_holding_item_view.ho_jsonb->>'temporaryLocationId' ~ '') IS NOT TRUE)) AND ( (instance_holding_item_view.it_jsonb->>'permanentLocationId' ~ '') IS NOT TRUE)) AND ( (instance_holding_item_view.it_jsonb->>'temporaryLocationId' ~ '') IS NOT TRUE))",
  "SELECT * FROM t WHERE (((lower(f_unaccent(instance_holding_item_view.jsonb->>'title')) ~ lower(f_unaccent('(^\\|[[:punct:]]|[[:space:]]|(?=[[:punct:]]|[[:space:]])).*($|[[:punct:]]|[[:space:]]|(?<=[[:punct:]]|[[:space:]]))'))) OR (lower(f_unaccent(instance_holding_item_view.jsonb->>'contributors')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]]|(?=[[:punct:]]|[[:space:]]))\\\"name\\\":([[:punct:]]|[[:space:]]) \\\".*\\\"($|[[:punct:]]|[[:space:]]|(?<=[[:punct:]]|[[:space:]]))')))) OR (lower(f_unaccent(instance_holding_item_view.jsonb->>'identifiers')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]]|(?=[[:punct:]]|[[:space:]]))\\\"value\\\":([[:punct:]]|[[:space:]]) \\\".*\\\"($|[[:punct:]]|[[:space:]]|(?<=[[:punct:]]|[[:space:]]))'))))",
  "SELECT * FROM t WHERE (((lower(f_unaccent(instance_holding_item_view.jsonb->>'title')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]]|(?=[[:punct:]]|[[:space:]])).*($|[[:punct:]]|[[:space:]]|(?<=[[:punct:]]|[[:space:]]))'))) OR (lower(f_unaccent(instance_holding_item_view.jsonb->>'contributors')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]]|(?=[[:punct:]]|[[:space:]]))\"name\":([[:punct:]]|[[:space:]]) \".*\"($|[[:punct:]]|[[:space:]]|(?<=[[:punct:]]|[[:space:]]))')))) OR (lower(f_unaccent(instance_holding_item_view.jsonb->>'identifiers')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]]|(?=[[:punct:]]|[[:space:]]))\"value\":([[:punct:]]|[[:space:]]) \".*\"($|[[:punct:]]|[[:space:]]|(?<=[[:punct:]]|[[:space:]]))')))) AND ((((((((true) AND ( (instance_holding_item_view.ho_jsonb->>'temporaryLocationId' ~ '') IS NOT TRUE)) AND ( (instance_holding_item_view.it_jsonb->>'permanentLocationId' ~ '') IS NOT TRUE)) AND ( (instance_holding_item_view.it_jsonb->>'temporaryLocationId' ~ '') IS NOT TRUE)) AND ((lower(f_unaccent(instance_holding_item_view.ho_jsonb->>'permanentLocationId')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]]|(?=[[:punct:]]|[[:space:]]))53cf956f-c1df-410b-8bea-27f712cca7c0($|[[:punct:]]|[[:space:]]|(?<=[[:punct:]]|[[:space:]]))'))) OR (lower(f_unaccent(instance_holding_item_view.ho_jsonb->>'permanentLocationId')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]]|(?=[[:punct:]]|[[:space:]]))fcd64ce1-6995-48f0-840e-89ffa2288371($|[[:punct:]]|[[:space:]]|(?<=[[:punct:]]|[[:space:]]))'))))) OR ((((true) AND ( (instance_holding_item_view.it_jsonb->>'permanentLocationId' ~ '') IS NOT TRUE)) AND ( (instance_holding_item_view.it_jsonb->>'temporaryLocationId' ~ '') IS NOT TRUE)) AND ((lower(f_unaccent(instance_holding_item_view.ho_jsonb->>'temporaryLocationId')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]]|(?=[[:punct:]]|[[:space:]]))53cf956f-c1df-410b-8bea-27f712cca7c0($|[[:punct:]]|[[:space:]]|(?<=[[:punct:]]|[[:space:]]))'))) OR (lower(f_unaccent(instance_holding_item_view.ho_jsonb->>'temporaryLocationId')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]]|(?=[[:punct:]]|[[:space:]]))fcd64ce1-6995-48f0-840e-89ffa2288371($|[[:punct:]]|[[:space:]]|(?<=[[:punct:]]|[[:space:]]))')))))) OR (((true) AND ( (instance_holding_item_view.it_jsonb->>'temporaryLocationId' ~ '') IS NOT TRUE)) AND ((lower(f_unaccent(instance_holding_item_view.it_jsonb->>'permanentLocationId')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]]|(?=[[:punct:]]|[[:space:]]))53cf956f-c1df-410b-8bea-27f712cca7c0($|[[:punct:]]|[[:space:]]|(?<=[[:punct:]]|[[:space:]]))'))) OR (lower(f_unaccent(instance_holding_item_view.it_jsonb->>'permanentLocationId')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]]|(?=[[:punct:]]|[[:space:]]))fcd64ce1-6995-48f0-840e-89ffa2288371($|[[:punct:]]|[[:space:]]|(?<=[[:punct:]]|[[:space:]]))')))))) OR ((lower(f_unaccent(instance_holding_item_view.it_jsonb->>'temporaryLocationId')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]]|(?=[[:punct:]]|[[:space:]]))53cf956f-c1df-410b-8bea-27f712cca7c0($|[[:punct:]]|[[:space:]]|(?<=[[:punct:]]|[[:space:]]))'))) OR (lower(f_unaccent(instance_holding_item_view.it_jsonb->>'temporaryLocationId')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]]|(?=[[:punct:]]|[[:space:]]))fcd64ce1-6995-48f0-840e-89ffa2288371($|[[:punct:]]|[[:space:]]|(?<=[[:punct:]]|[[:space:]]))'))))) ORDER BY lower(f_unaccent(instance_holding_item_view.jsonb->>'title')) LIMIT 30 OFFSET 0"};

  @Test
  public void parseQuery() throws JSQLParserException {
    for (int i = 0; i < queries.length; i++) {
      ParsedQuery pQ = PostgresClient.parseQuery(queries[i]);

      assertThat(pQ.getQueryWithoutLimOff(),
        not(either(containsString(notTrue)).or(
          containsString(isTrue))));

      assertThat(pQ.getCountQuery(),
        not(either(containsString(notTrue)).or(
          containsString(isTrue))));

      assertThat(pQ.getWhereClause(),
        not(either(containsString(notTrue)).or(
          containsString(isTrue))));
   }
 }

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
    JsonObject env = new JsonObject()
        .put("host", "example.com")
        .put("port", 9876);
    JsonObject config = PostgresClient.getPostgreSQLClientConfig("footenant", "aSchemaName", env);
    assertThat(config.getString("host"), is("example.com"));
    assertThat(config.getInteger("port"), is(9876));
    assertThat(config.getString("username"), is("aSchemaName"));
  }

  @Test
  public void configFile() throws Exception {
    // values from src/test/resources/my-postgres-conf.json
    PostgresClient.setConfigFilePath("/my-postgres-conf.json");
    JsonObject config = PostgresClient.getPostgreSQLClientConfig("public", null, empty);
    assertThat(config.getString("host"), is("localhost"));
    assertThat(config.getInteger("port"), is(5433));
    assertThat(config.getString("username"), is("postgres"));
  }

  @Test
  public void configFileTenant() throws Exception {
    // values from src/test/resources/my-postgres-conf.json
    PostgresClient.setConfigFilePath("/my-postgres-conf.json");
    JsonObject config = PostgresClient.getPostgreSQLClientConfig("footenant", "mySchemaName", empty);
    assertThat(config.getString("host"), is("localhost"));
    assertThat(config.getInteger("port"), is(5433));
    assertThat(config.getString("username"), is("mySchemaName"));
  }

  @Test
  public void testProcessResults() {
    PostgresClient testClient = PostgresClient.testClient();

    int total = 15;
    ResultSet rs = getMockTestPojoResultSet(total);

    List<TestPojo> results = testClient.processResults(rs, total, TestPojo.class, false).getResults();

    assertTestPojoResults(results, total);
  }

  @Test
  public void testDeserializeResults() {
    PostgresClient testClient = PostgresClient.testClient();

    int total = 25;
    ResultSet rs = getMockTestPojoResultSet(total);
    PostgresClient.ResultsHelper<TestPojo> resultsHelper = new PostgresClient.ResultsHelper<>(rs, total, TestPojo.class, false);

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
      "id", "foo", "bar", "biz", "baz"
    }));
    Map<String, Method> externalColumnSettters = testClient.getExternalColumnSetters(columnNames, TestPojo.class, false);
    assertThat(externalColumnSettters.size(), is(5));
    assertThat(externalColumnSettters.get("id"), is(TestPojo.class.getMethod(testClient.databaseFieldToPojoSetter("id"), String.class)));
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
    String id = "80c72dad-f88c-4d48-a516-9a0ab16a029b";
    String foo = "Hello";
    String bar = "World";
    Double biz = 1.0;
    List<String> baz = new ArrayList<String>(Arrays.asList(new String[] {
      "This", "is", "a", "test"
    }));
    JsonObject row = new JsonObject()
        .put("id", id)
        .put("foo", foo)
        .put("bar", bar)
        .put("biz", biz)
        .put("baz", baz);
    testClient.populateExternalColumns(externalColumnSettters, o, row);
    assertThat(o.getId(), is(id));
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
    QueryHelper queryHelper = new QueryHelper(false, "test_pojo", new ArrayList<FacetField>());
    queryHelper.selectQuery = "SELECT * FROM test_pojo";

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
        return testClient.processResults(totaledResults.set, totaledResults.total, TestPojo.class, false);
      },
      reply -> {
        List<TestPojo> results = reply.result().getResults();

        assertTestPojoResults(results, total);
      }
    );

  }

  @Test
  public void testPrepareCountQueryWithoutFacets() throws IOException, TemplateException {
    PostgresClient testClient = PostgresClient.testClient();
    QueryHelper queryHelper = new QueryHelper(false, "test_pojo", new ArrayList<FacetField>());
    queryHelper.selectQuery = "SELECT id, foo, bar FROM test_pojo LIMIT 10 OFFSET 1";
    testClient.prepareCountQuery(queryHelper);

    assertThat(queryHelper.countQuery, is("SELECT COUNT(*) FROM test_pojo"));
  }

  @Test
  public void testPrepareCountQueryWithFacets() throws IOException, TemplateException {
    PostgresClient testClient = PostgresClient.testClient();
    List<FacetField> facets = new ArrayList<FacetField>() {{
      add(new FacetField("jsonb->>'biz'"));
    }};
    QueryHelper queryHelper = new QueryHelper(false, "test_jsonb_pojo", facets);
    queryHelper.selectQuery = "SELECT id, foo, bar FROM test_jsonb_pojo LIMIT 10 OFFSET 1";
    testClient.prepareCountQuery(queryHelper);

    assertThat(queryHelper.selectQuery.replace("\r\n", "\n"), is(
      "with facets as (\n" +
      "    SELECT id, foo, bar FROM test_jsonb_pojo  \n" +
      "     LIMIT 10000 \n" +
      " )\n" +
      " ,\n" +
      " count_on as (\n" +
      "    SELECT\n" +
      "      test_raml_module_builder.count_estimate_smart2(count(*) , 10000, 'SELECT COUNT(*) FROM test_jsonb_pojo') AS count\n" +
      "    FROM facets\n" +
      " )\n" +
      " ,\n" +
      " grouped_by as (\n" +
      "    SELECT\n" +
      "        jsonb->>'biz' as biz,\n" +
      "      count(*) as count\n" +
      "    FROM facets\n" +
      "    GROUP BY GROUPING SETS (\n" +
      "        biz    )\n" +
      " )\n" +
      " ,\n" +
      "   lst1 as(\n" +
      "     SELECT\n" +
      "        jsonb_build_object(\n" +
      "            'type' , 'biz',\n" +
      "            'facetValues',\n" +
      "            json_build_array(\n" +
      "                jsonb_build_object(\n" +
      "                'value', biz,\n" +
      "                'count', count)\n" +
      "            )\n" +
      "        ) AS jsonb,\n" +
      "        count as count\n" +
      "    FROM grouped_by\n" +
      "     where biz is not null\n" +
      "     group by biz, count\n" +
      "     order by count desc\n" +
      "     )\n" +
      ",\n" +
      "ret_records as (\n" +
      "       select _id as _id, jsonb  FROM facets\n" +
      "       )\n" +
      "  (SELECT '00000000-0000-0000-0000-000000000000'::uuid as _id, jsonb FROM lst1 limit 5)\n" +
      "  \n" +
      "  UNION\n" +
      "  (SELECT '00000000-0000-0000-0000-000000000000'::uuid as _id,  jsonb_build_object('count' , count) FROM count_on)\n" +
      "  UNION ALL\n" +
      "  (select _id as _id, jsonb from ret_records  LIMIT 10  OFFSET 1);\n")
    );
    assertThat(queryHelper.countQuery, is("SELECT COUNT(*) FROM test_jsonb_pojo"));
  }

  @Test
  public void testProcessQuery() {
    PostgresClient testClient = PostgresClient.testClient();
    List<FacetField> facets = new ArrayList<FacetField>() {{
      add(new FacetField("jsonb->>'biz'"));
    }};
    QueryHelper queryHelper = new QueryHelper(false, "test_jsonb_pojo", facets);
    queryHelper.selectQuery = "SELECT id, foo, bar FROM test_jsonb_pojo LIMIT 30 OFFSET 1";

    int total = 30;

    SQLConnection connection = new PostgreSQLConnectionImpl(null, null, null) {
      @Override
      public SQLConnection query(String sql, Handler<AsyncResult<ResultSet>> handler) {
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
      totaledResults -> testClient.processResults(totaledResults.set, totaledResults.total, TestJsonbPojo.class, false),
      reply -> {
        List<TestJsonbPojo> results = reply.result().getResults();

        assertTestJsonbPojoResults(results, total);
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

}
