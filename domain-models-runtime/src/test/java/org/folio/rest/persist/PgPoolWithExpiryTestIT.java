package org.folio.rest.persist;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class PgPoolWithExpiryTestIT extends PostgresClientITBase {
  @Rule
  public Timeout timeoutRule = Timeout.seconds(10);

  @Test
  public void testExpiryGet(TestContext context) {
    JsonObject configuration = PostgresClient.getInstance(vertx, tenant).getPostgreSQLClientConfig();
    PostgresClient.getInstance(vertx, tenant).select("SELECT 2", context.asyncAssertSuccess(res0 -> {
      configuration.put("connectionReleaseDelay", 1);
      PgPoolWithExpiry pgPoolWithExpiry = PostgresClient.createPgPoolWithExpiry(vertx, configuration);
      pgPoolWithExpiry.get().getConnection(context.asyncAssertSuccess(con ->
          vertx.setTimer(20, res1 ->
              con.query("SELECT 1", context.asyncAssertSuccess(res2 -> pgPoolWithExpiry.close())))));
    }));
  }

  @Test
  public void testSet(TestContext context) {
    JsonObject configuration = PostgresClient.getInstance(vertx, tenant).getPostgreSQLClientConfig();
    PostgresClient.getInstance(vertx, tenant).select("SELECT 2", context.asyncAssertSuccess(res0 -> {
      configuration.put("connectionReleaseDelay", 1);
      PgPoolWithExpiry pgPoolWithExpiry = PostgresClient.createPgPoolWithExpiry(vertx, configuration);
      pgPoolWithExpiry.set(pgPoolWithExpiry.get());
      pgPoolWithExpiry.get().getConnection(context.asyncAssertSuccess(con ->
          vertx.setTimer(20, res1 ->
              con.query("SELECT 1", context.asyncAssertSuccess(res2 -> pgPoolWithExpiry.close())))));
    }));
  }

}
