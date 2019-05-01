package org.folio.rest.persist.ddlgen;


import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.folio.rest.persist.PostgresClient;

import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.tools.utils.VertxUtils;
import org.folio.util.ResourceUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import freemarker.template.TemplateException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class SchemaMakerIT  {
  /** If we start and stop our own embedded postgres */
  static private boolean ownEmbeddedPostgres = false;
  static private final String schema = "harvard_circ";
  static private Vertx vertx;

  @BeforeClass
  public static void setUpClass(TestContext context) throws Exception {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    startEmbeddedPostgres(vertx);
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    if (ownEmbeddedPostgres) {
      PostgresClient.stopEmbeddedPostgres();
    }

    vertx.close(context.asyncAssertSuccess());
  }

  public static void startEmbeddedPostgres(Vertx vertx) throws IOException {
    if (PostgresClient.isEmbedded()) {
      // starting and stopping embedded postgres is done by someone else
      return;
    }

    // Read configuration
    PostgresClient postgresClient = PostgresClient.getInstance(vertx);

    if (! PostgresClient.isEmbedded()) {
      // some external postgres
      return;
    }

    postgresClient.startEmbeddedPostgres();

    // We started our own embedded postgres, we also need to stop it.
    ownEmbeddedPostgres = true;
  }
  @Test
  public void canMakeAuditedTable(TestContext context) throws IOException, TemplateException {
    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.UPDATE, "mod-foo-18.2.3",
        "mod-foo-18.2.4");
    setUpSchema(context);
    String json = ResourceUtil.asString("templates/db_scripts/schemaWithAudit.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
    // assertions here
    String result = schemaMaker.generateDDL();
    execute(context, result);
    
    String table = "harvard_circ.test_tenantapi";
    int size = 5;
    //create entries inside new table
    
      String sql = "INSERT INTO " + table +
        " SELECT md5(username)::uuid, json_build_object('username', username, 'id', md5(username)::uuid)" +
        "  FROM (SELECT '" + Math.floor(Math.random() * size)  + " ' || generate_series(1,5) AS username) AS subquery";
      execute(context, sql);
    
    //retrieve audit entries
    String auditTable = "harvard_circ.audit_test_tenantapi";
    String retrieveSql = "select * from " + auditTable + " ";
    execute(context,retrieveSql, reply -> {
      assertTrue(reply.result().size() > 0);
    });
  }
  


  private void setUpSchema(TestContext context) {
    execute(context, "CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;");
    execute(context, "DROP SCHEMA IF EXISTS " + schema + " CASCADE;");
    executeIgnore(context, "CREATE ROLE " + schema + " PASSWORD 'testtenant' NOSUPERUSER NOCREATEDB INHERIT LOGIN;");
    execute(context, "CREATE SCHEMA " + schema + " AUTHORIZATION " + schema);
    execute(context, "GRANT ALL PRIVILEGES ON SCHEMA " + schema + " TO " + schema);
    execute(context, "CREATE OR REPLACE FUNCTION f_unaccent(text) RETURNS text AS $func$ SELECT public.unaccent('public.unaccent', $1) $func$ LANGUAGE sql IMMUTABLE;");
  }
  private static void execute(TestContext context, String sql) {
    Async async = context.async();
    PostgresClient.getInstance(vertx).getClient().querySingle(sql, reply -> {
      if (reply.failed()) {
        async.complete();
        context.fail(reply.cause());
      }
      async.complete();
    });
    async.await();
  }
  private static void executeIgnore(TestContext context, String sql) {
    Async async = context.async();
    PostgresClient.getInstance(vertx).getClient().querySingle(sql, reply -> {
      async.complete();
    });
    async.await();
  }
  private static void execute(TestContext context, String sql, Handler<AsyncResult<JsonArray>> asyncResultHandler) {
    Async async = context.async();
    PostgresClient.getInstance(vertx).getClient().querySingle(sql, reply -> {
      if (reply.failed()) {
        async.complete();
        context.fail(reply.cause());
      }
      asyncResultHandler.handle(reply);
      async.complete();
    });
    async.await();
  }
}
