package org.folio.rest.persist.ddlgen;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientITBase;
import org.folio.rest.tools.utils.ObjectMapperTool;
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
      runSqlFileAsSuperuser(context, schemaMaker.generateDDL());
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
                  context.assertEquals(4, result.getNumRows());
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
      context.assertEquals(id       , result.getString(0), "id");
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

  @Test
  public void foreignKey(TestContext context) {
    runSchema(context, TenantOperation.CREATE, "schemaInstanceItem.json");
  }
}
