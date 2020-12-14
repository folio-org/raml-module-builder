package org.folio.rest.persist.ddlgen;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import org.folio.dbschema.Schema;
import org.folio.dbschema.TenantOperation;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientITBase;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.util.ResourceUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class SchemaMakerIT extends PostgresClientITBase {
  @Rule
  public Timeout rule = Timeout.seconds(15);

  @Before
  public void wipeAll(TestContext context) {
    dropSchemaAndRole(context);
  }

  private void runSchema(TestContext context, TenantOperation tenantOperation, String filename) {
    try {
      SchemaMaker schemaMaker = new SchemaMaker(tenant, PostgresClient.getModuleName(),
          tenantOperation, "mod-foo-18.2.3", "mod-foo-18.2.4");
      String json = ResourceUtil.asString("templates/db_scripts/" + filename);
      schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
      String sql = schemaMaker.generateCreate();
      runSqlFileAsSuperuser(context, sql);
      sql = schemaMaker.generateSchemas();
      runSqlFileAsSuperuser(context, sql);
    } catch (Exception e) {
      context.fail(e);
    }
  }

  private int selectInteger(TestContext context, String sql) {
    AtomicInteger i = new AtomicInteger();

    Async async = context.async();
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenant);
    postgresClient.selectSingle(sql, context.asyncAssertSuccess(result -> {
      i.set(result.getInteger(0));
      async.complete();
    }));

    async.awaitSuccess(5000);
    return i.get();
  }

  private String selectText(TestContext context, String sql) {
    String s [] = new String [1];
    Async async = context.async();
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenant);
    postgresClient.selectSingle(sql, context.asyncAssertSuccess(result -> {
      s[0] = result.getString(0);
      async.complete();
    }));
    async.awaitSuccess(5000);
    return s[0];
  }

  private void auditedTableCanInsertUpdateDelete(TestContext context, String table, String field) {
    execute(context, "INSERT INTO " + table +
        " SELECT md5(username)::uuid, json_build_object('username', username, 'id', md5(username)::uuid)" +
        " FROM (SELECT 'user' || generate_series(1, 5) AS username) AS subquery");
    execute(context, "UPDATE " + table + " SET jsonb=jsonb_set(jsonb, '{foo}', to_jsonb('bar'::text))");
    execute(context, "UPDATE " + table + " SET jsonb=jsonb_set(jsonb, '{foo}', to_jsonb('baz'::text))");
    execute(context, "UPDATE " + table + " SET jsonb=jsonb_set(jsonb, '{foo}', to_jsonb('bar'::text))");
    execute(context, "UPDATE " + table + " SET jsonb=jsonb_set(jsonb, '{foo}', to_jsonb('baz'::text))");
    execute(context, "DELETE FROM " + table);

    String auditTable = table.replace(".", ".audit_");
    String count = "SELECT count(*) FROM " + auditTable;
    assertThat("total number of audit entries",
        selectInteger(context, count), is(30));
    assertThat("total number of audit entries for insert",
        selectInteger(context, count + " WHERE jsonb->>'operation'='I' AND jsonb->'" + field + "'->>'foo' IS NULL"), is(5));
    assertThat("total number of audit entries for update",
        selectInteger(context, count + " WHERE jsonb->>'operation'='U' AND jsonb->'" + field + "'->>'foo' = 'bar'"), is(10));
    assertThat("total number of audit entries for update",
        selectInteger(context, count + " WHERE jsonb->>'operation'='U' AND jsonb->'" + field + "'->>'foo' = 'baz'"), is(10));
    assertThat("total number of audit entries for delete",
        selectInteger(context, count + " WHERE jsonb->>'operation'='D' AND jsonb->'" + field + "'->>'foo' = 'baz'"), is(5));
    assertThat("number of user2 audit entries",
        selectInteger(context, count + " WHERE jsonb->'" + field + "'->>'id' = md5('user2')::uuid::text"), is(6));
  }

  @Test
  public void canMakeAuditedTable(TestContext context) throws Exception {
    // We need to create two different audited tables to check that "CREATE AGGREGATE" in uuid.ftl
    // is called only once per aggregate function.
    runSchema(context, TenantOperation.CREATE, "schemaWithAudit.json");
    auditedTableCanInsertUpdateDelete(context, schema + ".test_tenantapi", "testTenantapiAudit");
    auditedTableCanInsertUpdateDelete(context, schema + ".test_tenantapi2", "testTenantapiAudit2");
    // Check that '"withAuditing": true' implicitly creates the table 'audit_test_implicit'
    // without an explicit entry in the schema.json table list
    auditedTableCanInsertUpdateDelete(context, schema + ".test_implicit", "implicitHistory");
  }

  @Test
  public void canConcurrentlyUseAuditedTable(TestContext context) {
    runSchema(context, TenantOperation.CREATE, "schemaWithAudit.json");
    String table = schema + ".test_tenantapi";
    execute(context, "INSERT INTO " + table +
        " SELECT md5(username)::uuid, json_build_object('username', username, 'id', md5(username)::uuid)" +
        " FROM (SELECT 'patron' || generate_series(1, 2) AS username) AS subquery");

    // concurrently update two records: https://issues.folio.org/browse/RMB-430
    Async async = context.async();
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenant);
    postgresClient.startTx(tx1 -> {
      context.assertTrue(tx1.succeeded());
      postgresClient.startTx(tx2 -> {
        context.assertTrue(tx2.succeeded());
        String sql = "UPDATE " + table + " SET jsonb=jsonb_set(jsonb, '{x}', to_jsonb('y'::text)) WHERE jsonb->>'username' = ";
        postgresClient.execute(tx1, sql + "'patron1'", context.asyncAssertSuccess(update1 -> {
          postgresClient.execute(tx2, sql + "'patron2'", context.asyncAssertSuccess(update2 -> {
            postgresClient.endTx(tx1, context.asyncAssertSuccess(endTx1 -> {
              postgresClient.endTx(tx2, context.asyncAssertSuccess(endTx2 -> {
                String sql3 = "SELECT * FROM " + table.replace(".", ".audit_");
                postgresClient.select(sql3, context.asyncAssertSuccess(result -> {
                  context.assertEquals(4, result.rowCount());
                  async.complete();
                }));
              }));
            }));
          }));
        }));
      });
    });
  }

  private boolean triggerExists(TestContext context, String name) {
    AtomicBoolean exists = new AtomicBoolean();
    Async async = context.async();
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenant);
    postgresClient.selectSingle(
        "SELECT count(*) FROM pg_trigger " +
        "WHERE tgrelid = '" + schema + ".test_tenantapi'::regclass AND tgname='" + name + "'",
        context.asyncAssertSuccess(count -> {
          exists.set(count.getInteger(0) == 1);
          async.complete();
    }));
    async.await(5000);
    return exists.get();
  }

  private void assertMetadataTrigger(TestContext context, boolean expected) {
    context.assertEquals(expected, triggerExists(context, "set_test_tenantapi_md_trigger"));
    context.assertEquals(expected, triggerExists(context, "set_test_tenantapi_md_json_trigger"));
  }

  @Test
  public void canCreateMetadataTriggerTrueTrue(TestContext context) throws Exception {
    runSchema(context, TenantOperation.CREATE, "schema.json");
    assertMetadataTrigger(context, true);
    runSchema(context, TenantOperation.UPDATE, "schema.json");
    assertMetadataTrigger(context, true);
  }

  @Test
  public void canCreateWithoutTriggerFalseFalse(TestContext context) throws Exception {
    runSchema(context, TenantOperation.CREATE, "schemaWithoutMetadata.json");
    assertMetadataTrigger(context, false);
    runSchema(context, TenantOperation.UPDATE, "schemaWithoutMetadata.json");
    assertMetadataTrigger(context, false);
  }

  @Test
  public void canCreateMetadataTriggerTrueFalse(TestContext context) throws Exception {
    runSchema(context, TenantOperation.CREATE, "schema.json");
    assertMetadataTrigger(context, true);
    runSchema(context, TenantOperation.UPDATE, "schemaWithoutMetadata.json");
    assertMetadataTrigger(context, false);
  }

  @Test
  public void canCreateWithoutTriggerFalseTrue(TestContext context) throws Exception {
    runSchema(context, TenantOperation.CREATE, "schemaWithoutMetadata.json");
    assertMetadataTrigger(context, false);
    runSchema(context, TenantOperation.UPDATE, "schema.json");
    assertMetadataTrigger(context, true);
  }

  private void assertIdJsonb(TestContext context, String id, String idInJsonb) {
    Async async = context.async();
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenant);
    String sql = "SELECT id, jsonb->>'id' FROM " + schema + ".test_tenantapi";
    postgresClient.selectSingle(sql, context.asyncAssertSuccess(result -> {
      context.assertEquals(id       , result.getUUID(0).toString(), "id");
      context.assertEquals(idInJsonb, result.getString(1), "jsonb->>'id'");
      async.complete();
    }));
    async.awaitSuccess(5000 /* ms */);
  }

  @Test
  public void canSetIdInJsonb(TestContext context) throws Exception {
    runSchema(context, TenantOperation.CREATE, "schemaWithAudit.json");
    String table = schema + ".test_tenantapi";
    String uuid1 = UUID.randomUUID().toString();
    String uuid2 = UUID.randomUUID().toString();
    executeSuperuser(context, "INSERT INTO " + table + " VALUES ('" + uuid1 + "', '{}')");
    assertIdJsonb(context, uuid1, uuid1);
    executeSuperuser(context, "UPDATE " + table + " SET jsonb='{\"id\":\"" + uuid2 + "\"}'");
    assertIdJsonb(context, uuid1, uuid1);
    executeSuperuser(context, "UPDATE " + table + " SET id='" + uuid2 + "'");
    assertIdJsonb(context, uuid2, uuid2);
  }

  @Test
  public void canCreateCompoundIndexes(TestContext context) throws Exception {
    runSchema(context, TenantOperation.CREATE, "compoundIndex.json");
  }

  private int countConstraints(TestContext context) {
    return selectInteger(context, "SELECT count(*) FROM pg_catalog.pg_constraint"
        + " WHERE conname LIKE 'holdingsrecordid_holdings_record_fkey%'"
        + " AND connamespace = (SELECT oid FROM pg_catalog.pg_namespace"
        + "                     WHERE nspname = 'sometenant_raml_module_builder')");
  }

  @Test
  public void foreignKey(TestContext context) {
    runSchema(context, TenantOperation.CREATE, "schemaInstanceItem.json");
    // create 2 duplicate constraints
    String sql = "ALTER TABLE sometenant_raml_module_builder.item"
        + " ADD CONSTRAINT holdingsRecordId_holdings_record_fkey%d"
        + " FOREIGN KEY (holdingsRecordId)"
        + " REFERENCES sometenant_raml_module_builder.holdings_record";
    executeSuperuser(context, String.format(sql, 1));
    executeSuperuser(context, String.format(sql, 50));
    // pretend that the last install/upgrade was made with RMB 29.3.2
    executeSuperuser(context, "UPDATE sometenant_raml_module_builder.rmb_internal"
        + " SET jsonb = jsonb || jsonb_build_object('rmbVersion', '29.3.2')");
    assertThat(countConstraints(context), is(3));
    // should remove the 2 duplicate constraints
    runSchema(context, TenantOperation.UPDATE, "schemaInstanceItem.json");
    assertThat(countConstraints(context), is(1));
  }

  private void assertSelectSingle(TestContext context, String sql, String expected) {
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenant);
    postgresClient.selectSingle(sql, context.asyncAssertSuccess(result -> {
      context.assertEquals(expected, result.getString(0));
    }));
  }

  private void assertSchemaSelectSingle(TestContext context, String sql, String expected) {
    runSchema(context, TenantOperation.CREATE, "schema.json");
    assertSelectSingle(context, sql, expected);
  }

  @Test
  public void f_unaccent_combining_character(TestContext context) {
    runSchema(context, TenantOperation.CREATE, "schema.json");
    assertSelectSingle(context, "SELECT f_unaccent(E'a\\u0308 and a\\u0308')", "a and a");
    assertSelectSingle(context, "SELECT f_unaccent(E'b\\u20e2c\\u20e3d\\u20e4')", "bcd");
  }

  @Test
  public void concat_array_object_values(TestContext context) {
    assertSchemaSelectSingle(context, String.format(
        "SELECT concat_array_object_values('%s'::jsonb, 'f')",
        "[{},{'k':'v','f':'1'},{'k':'v'},{'f':'2'},{'k':'v','f':'3'}]".replace('\'', '"')),
        "1 2 3");
  }

  @Test
  public void concat_array_object_values_filter(TestContext context) {
    assertSchemaSelectSingle(context, String.format(
        "SELECT concat_array_object_values('%s'::jsonb, 'f', 'k', 'v')",
        "[{},{'k':'v','f':'1'},{'k':'v'},{'f':'2'},{'k':'v','f':'3'}]".replace('\'', '"')),
        "1 3");
  }

  @Test
  public void first_array_object_values_filter(TestContext context) {
    assertSchemaSelectSingle(context, String.format(
        "SELECT first_array_object_value('%s'::jsonb, 'f', 'k', 'v')",
        "[{},{'k':'v','f':'1'},{'k':'v'},{'f':'2'},{'k':'v','f':'3'}]".replace('\'', '"')),
        "1");
  }

  @Test
  public void concat_array_object(TestContext context) {
    assertSchemaSelectSingle(context, String.format(
        "SELECT concat_array_object('%s'::jsonb)",
        "[{},3,'foo',{'a': 'b', 'c': 'd'}]".replace('\'', '"')),
        "{} 3 foo {'a': 'b', 'c': 'd'}".replace('\'', '"'));
  }

  private String indexdef(TestContext context, String indexname) {
    return selectText(context, "SELECT indexdef FROM pg_catalog.pg_indexes "
        + "WHERE indexname = '" + indexname + "' AND schemaname='" + schema + "'");
  }

  private int countCasetableIndexes(TestContext context) {
    return selectInteger(context, "SELECT count(*) FROM pg_catalog.pg_indexes "
        + "WHERE indexname LIKE 'casetable___idx%' AND schemaname='" + schema + "'");
  }

  @Test
  public void indexUpgrade(TestContext context) {
    runSchema(context, TenantOperation.CREATE, "indexRemoveAccents.json");
    assertThat(indexdef(context, "casetable_i_idx"),        containsString("lower(f_unaccent((jsonb ->> 'i'::text)))"));
    assertThat(indexdef(context, "casetable_u_idx_unique"), containsString("lower(f_unaccent((jsonb ->> 'u'::text)))"));
    assertThat(indexdef(context, "casetable_l_idx_like"),   containsString("lower(f_unaccent((jsonb ->> 'l'::text)))"));
    assertThat(indexdef(context, "casetable_g_idx_gin"),    containsString("lower(f_unaccent((jsonb ->> 'g'::text)))"));
    assertThat(indexdef(context, "casetable_f_idx_ft"),     containsString("get_tsvector(f_unaccent((jsonb ->> 'f'::text)))"));

    runSchema(context, TenantOperation.UPDATE, "indexKeepAccents.json");
    assertThat(indexdef(context, "casetable_i_idx"),        containsString("lower((jsonb ->> 'i'::text))"));
    assertThat(indexdef(context, "casetable_u_idx_unique"), containsString("lower((jsonb ->> 'u'::text))"));
    assertThat(indexdef(context, "casetable_l_idx_like"),   containsString("lower((jsonb ->> 'l'::text))"));
    assertThat(indexdef(context, "casetable_g_idx_gin"),    containsString("lower((jsonb ->> 'g'::text))"));
    assertThat(indexdef(context, "casetable_f_idx_ft"),     containsString("get_tsvector((jsonb ->> 'f'::text))"));

    // no indexes get recreated when schema doesn't change.
    execute(context, "DROP INDEX "
        + "casetable_i_idx, casetable_u_idx_unique, casetable_l_idx_like, casetable_g_idx_gin, casetable_f_idx_ft");
    runSchema(context, TenantOperation.UPDATE, "indexKeepAccents.json");
    assertThat(countCasetableIndexes(context), is(0));
  }

  @Test
  public void indexRename(TestContext context) {
    runSchema(context, TenantOperation.CREATE, "indexKeepAccents.json");
    assertThat(countCasetableIndexes(context), is(5));
    executeSuperuser(context, "CREATE INDEX casetable_i_idx_p        ON " + schema + ".casetable (('x'::text))");
    executeSuperuser(context, "CREATE INDEX casetable_u_idx_unique_p ON " + schema + ".casetable (('x'::text))");
    executeSuperuser(context, "CREATE INDEX casetable_l_idx_like_p   ON " + schema + ".casetable (('x'::text))");
    executeSuperuser(context, "CREATE INDEX casetable_g_idx_gin_p    ON " + schema + ".casetable (('x'::text))");
    executeSuperuser(context, "CREATE INDEX casetable_f_idx_ft_p     ON " + schema + ".casetable (('x'::text))");
    executeSuperuser(context, "INSERT INTO " + schema + ".rmb_internal_index "
        + "SELECT name || '_p', def, remove FROM " + schema + ".rmb_internal_index");
    assertThat(countCasetableIndexes(context), is(10));

    runSchema(context, TenantOperation.UPDATE, "indexKeepAccents.json");
    assertThat(countCasetableIndexes(context), is(5));
  }

  @Test
  public void indexDelete(TestContext context) {
    runSchema(context, TenantOperation.CREATE, "indexKeepAccents.json");
    assertThat(countCasetableIndexes(context), is(5));

    // delete two indexes using "tOps": "DELETE"
    // and delete two indexes by completely removing their entries from schema
    runSchema(context, TenantOperation.UPDATE, "indexDelete.json");
    assertThat(countCasetableIndexes(context), is(1));
  }

  @Test
  public void replacePublicSchemaFunctions(TestContext context) throws InterruptedException {
    runSchema(context, TenantOperation.CREATE, "schema.json");
    String indexdef = "CREATE INDEX foo ON " + schema + ".test_tenantapi USING btree "
        + "(COALESCE(public.f_unaccent((jsonb ->> 'foo'::text)), public.f_unaccent((jsonb ->> 'bar'::text))))";
    String sql = "CREATE OR REPLACE FUNCTION public.f_unaccent(text) RETURNS text AS 'SELECT $1' LANGUAGE sql;"
        + "UPDATE " + schema + ".rmb_internal SET jsonb = jsonb || '{\"rmbVersion\": \"29.1.0\"}'::jsonb;"
        + indexdef;
    runSqlFileAsSuperuser(context, sql);
    assertThat("indexdef before update", indexdef(context, "foo"), is(indexdef));

    runSchema(context, TenantOperation.UPDATE, "schema.json");
    // has "public.f_unaccent" been changed to "f_unaccent"?
    assertThat("indexdef after update", indexdef(context, "foo"), is(indexdef.replace("public.", "")));

    // run upgrade where rmbVersion suppresses index upgrade.
    sql = "DROP INDEX " + schema + ".foo;"
        + "UPDATE " + schema + ".rmb_internal SET jsonb = jsonb || '{\"rmbVersion\": \"X\"}'::jsonb;"
        + indexdef;
    runSqlFileAsSuperuser(context, sql);
    assertThat("indexdef before suppressed update", indexdef(context, "foo"), is(indexdef));
    runSchema(context, TenantOperation.UPDATE, "schema.json");
    assertThat("indexdef after suppressed update", indexdef(context, "foo"), is(indexdef));
  }
  
  @Test
  public void canCreateOptimisticLockingTrigger(TestContext context) {
    runSchema(context, TenantOperation.CREATE, "schemaWithOptimisticLocking.json");
    String olVersion = "_version";
    List<String> tables = Arrays.asList("tab_ol_off", "tab_ol_log", "tab_ol_fail", "tab_ol_none");
    String sql = "SELECT COALESCE((jsonb->>'%s')::numeric, 0) FROM %s";
    // test insert
    tables.forEach(table -> {
      execute(context, "INSERT INTO " + table +
          " SELECT md5(username)::uuid, json_build_object('username', username, 'id', md5(username)::uuid)" +
          " FROM (SELECT '" + table + "' AS username) AS subquery");
    });
    assertThat("tab_ol_off has no version", selectInteger(context, String.format(sql, olVersion, "tab_ol_off")), is(0));
    assertThat("tab_ol_log has version 1", selectInteger(context, String.format(sql, olVersion, "tab_ol_log")), is(1));
    assertThat("tab_ol_fail has version 1", selectInteger(context, String.format(sql, olVersion, "tab_ol_fail")), is(1));
    assertThat("tab_ol_none has no version", selectInteger(context, String.format(sql, olVersion, "tab_ol_none")), is(0));
    // test update
    tables.forEach(table -> {
      String updateSql = "UPDATE " + table + " SET jsonb=jsonb_set(jsonb, '{" + olVersion + "}', to_jsonb('2'::text))";
      if (table.equals("tab_ol_fail")) {
        // fail on conflict
        executeAndExpectFailure(context, updateSql, "Cannot update record", "because it has been changed");
      } else {
        execute(context, updateSql);
      }
    });
    assertThat("tab_ol_off has no version", selectInteger(context, String.format(sql, olVersion, "tab_ol_off")), is(2));
    assertThat("tab_ol_log has version 1", selectInteger(context, String.format(sql, olVersion, "tab_ol_log")), is(2));
    assertThat("tab_ol_fail has version 1", selectInteger(context, String.format(sql, olVersion, "tab_ol_fail")), is(1));
    // update version as provided if table has no optimistic locking configuration
    assertThat("tab_ol_none has no version", selectInteger(context, String.format(sql, olVersion, "tab_ol_none")), is(2));
    // test insert with OFF
    execute(context, "DELETE from tab_ol_off");
    execute(context, "INSERT INTO tab_ol_off SELECT md5('abc')::uuid, jsonb_build_object('" + olVersion + "', 5)");
    assertThat("tab_ol_off ignore version", selectInteger(context, String.format(sql, olVersion, "tab_ol_off")), is(5));
    // turn off failOnConfict and test again
    runSchema(context, TenantOperation.UPDATE, "schemaWithOptimisticLocking2.json");
    execute(context, "UPDATE tab_ol_fail SET jsonb=jsonb_set(jsonb, '{" + olVersion + "}', to_jsonb('5'::text))");
    assertThat("tab_ol_fail has version 1", selectInteger(context, String.format(sql, olVersion, "tab_ol_fail")), is(5));
  }
  
  private static void executeAndExpectFailure(TestContext context, String sqlStatement, String ... errMessages) {
      PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenant);
      postgresClient.execute(sqlStatement, context.asyncAssertFailure(cause -> {
        for (String errMessage : errMessages) {
          context.assertTrue(cause.getMessage().contains(errMessage));
        }
      }));
  }
}
