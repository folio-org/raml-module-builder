package org.folio.rest.persist.ddlgen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientITBase;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.tools.utils.VertxUtils;
import org.folio.util.ResourceUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import freemarker.template.TemplateException;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class SchemaMakerIT extends PostgresClientITBase {
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
    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.UPDATE,
        "mod-foo-18.2.3", "mod-foo-18.2.4");
    execute(context, "DROP SCHEMA IF EXISTS " + schema + " CASCADE;");
    executeIgnore(context, "CREATE ROLE " + schema + " PASSWORD 'testtenant' NOSUPERUSER NOCREATEDB INHERIT LOGIN;");
    execute(context, "CREATE SCHEMA " + schema + " AUTHORIZATION " + schema);
    execute(context, "GRANT ALL PRIVILEGES ON SCHEMA " + schema + " TO " + schema);
    execute(context, "CREATE OR REPLACE FUNCTION f_unaccent(text) RETURNS text AS $func$ SELECT public.unaccent('public.unaccent', $1) $func$ LANGUAGE sql IMMUTABLE;");
      String json = ResourceUtil.asString("templates/db_scripts/schemaWithAudit.json");
      schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
      //assertions here
      String result = schemaMaker.generateDDL();
      
      execute(context, result);
  }
  private static void execute(TestContext context, String sql) {
    Async async = context.async();
    PostgresClient.getInstance(vertx).getClient().querySingle(sql, reply -> {
      if (reply.failed()) {
        context.fail(reply.cause());
      }
      async.complete();
    });
    async.await();
  }
}
