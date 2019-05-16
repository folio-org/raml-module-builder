package org.folio.rest.persist;

import java.util.Collections;
import java.util.Map;

import org.folio.rest.tools.utils.VertxUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public class PostgresClientITBase {
  protected static final String tenant = "sometenant";
  protected static final Map<String,String> okapiHeaders = Collections.singletonMap("x-okapi-tenant", tenant);
  protected static final String schema = PostgresClient.convertToPsqlStandard(tenant);
  protected static Vertx vertx;

  protected static void setUpClass(TestContext context) throws Exception {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    dropSchemaAndRole(context);
    executeSuperuser(context,
        "CREATE ROLE " + schema + " PASSWORD '" + tenant + "' NOSUPERUSER NOCREATEDB INHERIT LOGIN",
        "CREATE SCHEMA " + schema + " AUTHORIZATION " + schema,
        "GRANT ALL PRIVILEGES ON SCHEMA " + schema + " TO " + schema);
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
  }

  public static void dropSchemaAndRole(TestContext context) {
    executeSuperuser(context, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
    executeSuperuserIgnore(context, "DROP OWNED BY " + schema + " CASCADE");
    executeSuperuser(context, "DROP ROLE IF EXISTS " + schema);
    // Prevent "aclcheck_error" "permission denied for schema ..."
    // when recreating the ROLE with the same name but a different role OID.
    PostgresClient.closeAllClients();
  }

  /**
   * Execute all statements, stop and fail if any fails.
   */
  public static void execute(TestContext context, String ... sqlStatements) {
    for (String sql : sqlStatements) {
      Async async = context.async();
      PostgresClient.getInstance(vertx, tenant).execute(sql, reply -> {
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
      PostgresClient.getInstance(vertx, tenant).execute(sql, reply -> {
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
      PostgresClient.getInstance(vertx).getClient().querySingle(sql, reply -> {
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
      PostgresClient.getInstance(vertx).getClient().querySingle(sql, reply -> {
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
      context.fail(new RuntimeException(result.get(0) + ". SQL File: " + sqlFile));
    }));
    async.awaitSuccess();
  }
}
