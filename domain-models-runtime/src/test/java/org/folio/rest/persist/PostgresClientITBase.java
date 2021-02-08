package org.folio.rest.persist;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.folio.dbschema.Schema;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.persist.ddlgen.SchemaMaker;
import org.folio.dbschema.TenantOperation;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.rest.tools.utils.VertxUtils;
import org.folio.util.ResourceUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import freemarker.template.TemplateException;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public class PostgresClientITBase {
  protected static String tenant;
  protected static Map<String,String> okapiHeaders;
  protected static String schema;
  protected static Vertx vertx;

  static {
    setTenant("sometenant");
  }

  public static void setTenant(String tenant) {
    PostgresClientITBase.tenant = tenant;
    okapiHeaders = Collections.singletonMap("x-okapi-tenant", tenant);
    schema = PostgresClient.convertToPsqlStandard(tenant);
  }

  protected static void setUpClass(TestContext context) throws Exception {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    dropSchemaAndRole(context);
    executeSuperuser(context,
        "CREATE ROLE " + schema + " PASSWORD '" + tenant + "' NOSUPERUSER NOCREATEDB INHERIT LOGIN",
        "CREATE SCHEMA " + schema + " AUTHORIZATION " + schema,
        "GRANT ALL PRIVILEGES ON SCHEMA " + schema + " TO " + schema);
    LoadGeneralFunctions.loadFuncs(context, PostgresClient.getInstance(vertx), schema);
  }

  @BeforeClass
  public static void beforeClass(TestContext context) throws Exception {
    setUpClass(context);
  }

  protected static void tearDownClass(TestContext context) {
    dropSchemaAndRole(context);
    vertx.close(context.asyncAssertSuccess());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    tearDownClass(context);
    setTenant("sometenant");
  }

  public static void dropSchemaAndRole(TestContext context) {
    executeSuperuser(context, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
    executeSuperuserIgnore(context, "DROP OWNED BY " + schema + " CASCADE");
    executeSuperuser(context, "DROP ROLE IF EXISTS " + schema);
    // Prevent "aclcheck_error" "permission denied for schema"
    // when recreating the ROLE with the same name but a different role OID.
    PostgresClient.closeAllClients();
  }

  private static PostgresClient postgresClient() {
    if (PostgresClient.DEFAULT_SCHEMA.equals(tenant)) {
      return PostgresClient.getInstance(vertx);
    }
    return PostgresClient.getInstance(vertx, tenant);
  }

  /**
   * Execute all statements, stop and fail if any fails.
   */
  public static void execute(TestContext context, String ... sqlStatements) {
    for (String sql : sqlStatements) {
      Async async = context.async();
      postgresClient().execute(sql, reply -> {
        if (reply.failed()) {
          context.fail(new RuntimeException(reply.cause().getMessage() + ". SQL: " + sql, reply.cause()));
        } else {
          async.complete();
        }
      });
      async.awaitSuccess();
    }
  }

  /**
   * Execute all statements, ignore any failure.
   */
  public static void executeIgnore(TestContext context, String ... sqlStatements) {
    for (String sql : sqlStatements) {
      Async async = context.async();
      postgresClient().execute(sql, reply -> {
        async.complete();
      });
      async.await();
    }
  }

  /**
   * Execute all statements as superuser, stop and fail if any fails.
   */
  public static void executeSuperuser(TestContext context, String ... sqlStatements) {
    for (String sql : sqlStatements) {
      Async async = context.async();
      PostgresClientHelper.getClient(PostgresClient.getInstance(vertx)).query(sql).execute(reply -> {
        if (reply.failed()) {
          context.fail(new RuntimeException(reply.cause().getMessage() + ". SQL: " + sql, reply.cause()));
        }
        async.complete();
      });
      async.awaitSuccess();
    }
  }

  /**
   * Execute all statements as superuser, ignore any failure.
   */
  public static void executeSuperuserIgnore(TestContext context, String ... sqlStatements) {
    for (String sql : sqlStatements) {
      Async async = context.async();
      PostgresClientHelper.getClient(PostgresClient.getInstance(vertx)).query(sql).execute(reply -> {
        async.complete();
      });
      async.await();
    }
  }

  /**
   * Run sqlFile as database superuser.
   */
  public static void runSqlFileAsSuperuser(TestContext context, String sqlFile) {
    Async async = context.async();
    PostgresClient.getInstance(vertx).runSQLFile(sqlFile, true, context.asyncAssertSuccess(result -> {
      if (result.isEmpty()) {
        async.complete();
        return;
      }
      context.fail(new RuntimeException(result.get(0) + "\n### SQL File: ###\n" + sqlFile + "\n### end of SQL File ###"));
    }));
    async.awaitSuccess();
  }

  /**
   * Run the DDL SQL from SchemaMaker as defined by schema.json.
   */
  public static void runSchemaMaker(TestContext context) {
    runSchemaMaker(context, "templates/db_scripts/schema.json");
  }

  /**
   * Run the DDL SQL from SchemaMaker as defined by the schemaJsonFilename.
   */
  public static void runSchemaMaker(TestContext context, String schemaJsonFilename) {
    SchemaMaker schemaMaker = new SchemaMaker(tenant, PostgresClient.getModuleName(),
        TenantOperation.CREATE, null, null);
    String json = ResourceUtil.asString(schemaJsonFilename);
    try {
      schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
      runSqlFileAsSuperuser(context, schemaMaker.generateCreate());
      runSqlFileAsSuperuser(context, schemaMaker.generateSchemas());
    } catch (IOException|TemplateException e) {
      context.fail(e);
    }
  }
}
