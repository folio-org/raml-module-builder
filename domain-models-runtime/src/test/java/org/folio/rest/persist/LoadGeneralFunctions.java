package org.folio.rest.persist;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import java.io.IOException;
import org.apache.commons.io.IOUtils;

public class LoadGeneralFunctions {
  static private void load(TestContext context, PostgresClient postgresClient, String rep, String file) {
    Async async = context.async();
    try {
      String sql = IOUtils.toString(
        LoadGeneralFunctions.class.getClassLoader().getResourceAsStream("templates/db_scripts/" + file), "UTF-8");
      sql = sql.replace("${myuniversity}_${mymodule}.", rep.isEmpty() ? "" : (rep + "."));
      sql = sql.replace("${myuniversity}_${mymodule}", rep);
      sql = sql.replace("${exactCount}", "1000");
      postgresClient.getClient().query(sql).execute(context.asyncAssertSuccess(reply -> async.complete()));
    } catch (IOException ex) {
      context.fail(ex);
    }
    async.awaitSuccess(1000);
  }

  static void loadFuncs(TestContext context, PostgresClient postgresClient, String rep) {
    load(context, postgresClient, rep, "extensions.ftl");
    load(context, postgresClient, rep, "general_functions.ftl");
  }
}
