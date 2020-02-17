package org.folio.rest.persist;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import java.io.IOException;
import org.apache.commons.io.IOUtils;

public class LoadGeneralFunctions {
  static void loadFuncs(TestContext context, PostgresClient postgresClient, String rep) {
    Async async = context.async();
    try {
      String sql = IOUtils.toString(
        LoadGeneralFunctions.class.getClassLoader().getResourceAsStream("templates/db_scripts/general_functions.ftl"), "UTF-8");
      sql = sql.replace("${myuniversity}_${mymodule}.", rep);
      sql = sql.replace("${exactCount}", "100");
      postgresClient.getClient().update(sql, context.asyncAssertSuccess(reply -> async.complete()));
    } catch (IOException ex) {
      context.fail(ex);
    }
    async.awaitSuccess(1000);
  }
}
