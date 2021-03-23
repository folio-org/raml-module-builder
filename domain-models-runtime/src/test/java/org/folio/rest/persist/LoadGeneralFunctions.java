package org.folio.rest.persist;

import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.folio.util.ResourceUtil;

public class LoadGeneralFunctions {

  static private Future<Void> load(PostgresClient postgresClient, String schema, String file) {
    String sql = ResourceUtil.asString("templates/db_scripts/" + file);
    sql = sql.replace("${myuniversity}_${mymodule}.", schema.isEmpty() ? "" : (schema + "."));
    sql = sql.replace("${myuniversity}_${mymodule}", schema);
    sql = sql.replace("${exactCount}", "1000");
    return postgresClient.execute(sql).mapEmpty();
  }

  static Future<Void> loadFuncs(PostgresClient postgresClient, String schema) {
    return load(postgresClient, schema, "extensions.ftl")
    .compose(x -> load(postgresClient, schema, "general_functions.ftl"));
  }

  static void loadFuncs(TestContext context, PostgresClient postgresClient, String schema) {
    Async async = context.async();
    loadFuncs(postgresClient, schema)
    .onComplete(x -> async.complete());
    async.awaitSuccess(1000);
  }
}
