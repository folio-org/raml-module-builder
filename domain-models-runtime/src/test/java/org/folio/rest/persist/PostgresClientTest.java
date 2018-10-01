package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import org.folio.rest.persist.facets.ParsedQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.core.json.JsonObject;

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
    PostgresClient.setConfigFilePath("nonexisting");
    JsonObject config = PostgresClient.getPostgreSQLClientConfig(/* default schema = */ "public", null, empty);
    assertThat(PostgresClient.isEmbedded(), is(true));
    assertThat(config.getString("host"), is("127.0.0.1"));
    assertThat(config.getInteger("port"), is(6000));
    assertThat(config.getString("username"), is("username"));
  }

  @Test
  public void configDefaultWithPortAndTenant() throws Exception {
    PostgresClient.setConfigFilePath("nonexisting");
    PostgresClient.setEmbeddedPort(5555);
    JsonObject config = PostgresClient.getPostgreSQLClientConfig("footenant", null, empty);
    assertThat(PostgresClient.isEmbedded(), is(true));
    assertThat(config.getString("host"), is("127.0.0.1"));
    assertThat(config.getInteger("port"), is(5555));
    assertThat(config.getString("username"), is("username"));
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
    JsonObject config = PostgresClient.getPostgreSQLClientConfig("public", null, empty);
    // values from src/test/resources/postgres-conf.json
    assertThat(config.getString("host"), is("localhost"));
    assertThat(config.getInteger("port"), is(5433));
    assertThat(config.getString("username"), is("postgres"));
  }

  @Test
  public void configFileTenant() throws Exception {
    JsonObject config = PostgresClient.getPostgreSQLClientConfig("footenant", "mySchemaName", empty);
    // values from src/test/resources/postgres-conf.json
    assertThat(config.getString("host"), is("localhost"));
    assertThat(config.getInteger("port"), is(5433));
    assertThat(config.getString("username"), is("mySchemaName"));
  }
}
