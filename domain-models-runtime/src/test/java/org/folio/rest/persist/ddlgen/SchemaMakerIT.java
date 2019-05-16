package org.folio.rest.persist.ddlgen;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.UUID;

import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientITBase;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.util.ResourceUtil;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class SchemaMakerIT extends PostgresClientITBase {
  private void createSchema(TestContext context, String schemaFilename) throws Exception {
    dropSchemaAndRole(context);
    SchemaMaker schemaMaker = new SchemaMaker(tenant, PostgresClient.getModuleName(),
        TenantOperation.CREATE, "mod-foo-18.2.3", "mod-foo-18.2.4");
    String json = ResourceUtil.asString(schemaFilename);
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
    runSqlFileAsSuperuser(context, schemaMaker.generateDDL());
  }

  private int selectInteger(TestContext context, String sql) {
    int [] i = new int [1];

    Async async = context.async();
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenant);
    postgresClient.selectSingle(sql, context.asyncAssertSuccess(result -> {
      i[0] = result.getInteger(0);
      async.complete();
    }));

    async.awaitSuccess();
    return i[0];
  }

  private void auditedTableCanInsertUpdateDelete(TestContext context, String table) {
    execute(context, "INSERT INTO " + table +
        " SELECT md5(username)::uuid, json_build_object('username', username, 'id', md5(username)::uuid)" +
        " FROM (SELECT 'user' || generate_series(1, 5) AS username) AS subquery");
    execute(context, "UPDATE " + table + " SET jsonb=jsonb_set(jsonb, '{foo}', to_jsonb('bar'::text))");
    execute(context, "DELETE FROM " + table);

    String auditTable = table.replace(".", ".audit_");
    String count = "SELECT count(*) FROM " + auditTable;
    assertThat("total number of audit entries",
        selectInteger(context, count), is(15));
    assertThat("total number of audit entries for insert",
        selectInteger(context, count + " WHERE operation='I' AND jsonb->>'foo' IS NULL"), is(5));
    assertThat("total number of audit entries for update",
        selectInteger(context, count + " WHERE operation='U' AND jsonb->>'foo' = 'bar'"), is(5));
    assertThat("total number of audit entries for delete",
        selectInteger(context, count + " WHERE operation='D' AND jsonb->>'foo' = 'bar'"), is(5));
  }

  @Test
  public void canMakeAuditedTable(TestContext context) throws Exception {
    // We need to create two different audited tables to check that "CREATE AGGREGATE" in uuid.ftl
    // is called only once per aggregate function.
    createSchema(context, "templates/db_scripts/schemaWithAudit.json");
    auditedTableCanInsertUpdateDelete(context, schema + ".test_tenantapi");
    auditedTableCanInsertUpdateDelete(context, schema + ".test_tenantapi2");
  }

  private void assertIdJsonb(TestContext context, String id, String idInJsonb) {
    Async async = context.async();
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenant);
    String sql = "SELECT _id, jsonb->>'id' FROM " + schema + ".test_tenantapi";
    postgresClient.selectSingle(sql, context.asyncAssertSuccess(result -> {
      context.assertEquals(id       , result.getString(0), "id");
      context.assertEquals(idInJsonb, result.getString(1), "jsonb->>'id'");
      async.complete();
    }));
    async.awaitSuccess(5000 /* ms */);
  }

  @Test
  public void canSetIdInJsonb(TestContext context) throws Exception {
    createSchema(context, "templates/db_scripts/schemaPopulateJsonWithId.json");
    String table = schema + ".test_tenantapi";
    String uuid1 = UUID.randomUUID().toString();
    String uuid2 = UUID.randomUUID().toString();
    executeSuperuser(context, "INSERT INTO " + table + " VALUES ('" + uuid1 + "', '{}')");
    assertIdJsonb(context, uuid1, uuid1);
    executeSuperuser(context, "UPDATE " + table + " SET jsonb='{\"id\":\"" + uuid2 + "\"}'");
    assertIdJsonb(context, uuid1, uuid1);
    executeSuperuser(context, "UPDATE " + table + " SET _id='" + uuid2 + "'");
    assertIdJsonb(context, uuid2, uuid2);
  }
}
