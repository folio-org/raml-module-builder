package org.folio.rest.persist.ddlgen;

import java.io.IOException;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientITBase;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.util.ResourceUtil;
import org.junit.Test;
import org.junit.runner.RunWith;

import freemarker.template.TemplateException;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class SchemaMakerIT extends PostgresClientITBase {
  private void auditedTableCanInsertUpdateDelete(TestContext context, String table) {
    execute(context, "INSERT INTO " + table +
        " SELECT md5(username)::uuid, json_build_object('username', username, 'id', md5(username)::uuid)" +
        " FROM (SELECT 'user' || generate_series(1, 5) AS username) AS subquery");
    execute(context, "UPDATE " + table + " SET jsonb=jsonb_set(jsonb, '{foo}', to_jsonb('bar'::text))");
    execute(context, "DELETE FROM " + table);

    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenant);
    String auditTable = table.replace(".", ".audit_");
    postgresClient.selectSingle("SELECT count(*) FROM " + auditTable, context.asyncAssertSuccess(result -> {
      context.assertEquals(15, result.getInteger(0), "total number of audit entries");
    }));
    postgresClient.selectSingle("SELECT count(*) FROM " + auditTable + " WHERE operation='I' AND jsonb->>'foo' IS NULL",
        context.asyncAssertSuccess(result -> {
      context.assertEquals(5, result.getInteger(0), "total number of audit entries for insert");
    }));
    postgresClient.selectSingle("SELECT count(*) FROM " + auditTable + " WHERE operation='U' AND jsonb->>'foo' = 'bar'",
        context.asyncAssertSuccess(result -> {
      context.assertEquals(5, result.getInteger(0), "total number of audit entries for update");
    }));
    postgresClient.selectSingle("SELECT count(*) FROM " + auditTable + " WHERE operation='D' AND jsonb->>'foo' = 'bar'",
        context.asyncAssertSuccess(result -> {
      context.assertEquals(5, result.getInteger(0), "total number of audit entries for delete");
    }));
  }

  @Test
  public void canMakeAuditedTable(TestContext context) throws IOException, TemplateException {
    // We need to create two different audited tables to check that "CREATE AGGREGATE" in uuid.ftl
    // is called only once per aggregate function.

    dropSchemaAndRole(context);

    SchemaMaker schemaMaker = new SchemaMaker(tenant, PostgresClient.getModuleName(),
        TenantOperation.CREATE, "mod-foo-18.2.3", "mod-foo-18.2.4");
    String json = ResourceUtil.asString("templates/db_scripts/schemaWithAudit.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
    runSqlFileAsSuperuser(context, schemaMaker.generateDDL());

    auditedTableCanInsertUpdateDelete(context, schema + ".test_tenantapi");
    auditedTableCanInsertUpdateDelete(context, schema + ".test_tenantapi2");
  }
}
