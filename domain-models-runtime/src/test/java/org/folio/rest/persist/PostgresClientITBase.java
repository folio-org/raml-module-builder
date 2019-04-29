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
  protected static final String tenant = "uuidtenant";
  protected static final Map<String,String> okapiHeaders = Collections.singletonMap("x-okapi-tenant", tenant);
  protected static final String schema = PostgresClient.convertToPsqlStandard(tenant);
  protected static Vertx vertx;

  protected static void setUpClass(TestContext context) throws Exception {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    dropSchemaAndRole(context);
    execute(context,
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
    execute(context, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
    executeIgnore(context, "DROP OWNED BY " + schema + " CASCADE");
    execute(context, "DROP ROLE IF EXISTS " + schema);
  }

  /**
   * Execute all statements, stop and fail if any fails.
   */
  public static void execute(TestContext context, String ... sqlStatements) {
    for (String sql : sqlStatements) {
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

  /**
   * Execute all statements, ignore any failure.
   */
  public static void executeIgnore(TestContext context, String ... sqlStatements) {
    for (String sql : sqlStatements) {
      Async async = context.async();
      PostgresClient.getInstance(vertx).getClient().querySingle(sql, reply -> {
        async.complete();
      });
      async.await();
    }
  }

}
