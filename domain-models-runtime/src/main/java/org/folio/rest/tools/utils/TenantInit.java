package org.folio.rest.tools.utils;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.TenantAttributes;

public class TenantInit {
  private static final Logger log = LogManager.getLogger(TenantInit.class);

  private TenantInit() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  public static Future<Void> purge(TenantClient client, int waitMs) {
    TenantAttributes at = new TenantAttributes().withPurge(true);
    return exec(client, at, waitMs);
  }

  public static Future<Void> exec(TenantClient client, TenantAttributes tenantAttributes, int waitMs) {
    Promise<Void> promise = Promise.promise();
    try {
      client.postTenant(tenantAttributes, res1 -> {
        if (res1.failed()) {
          promise.fail(res1.cause());
          return;
        }
        int status1 = res1.result().statusCode();
        if (status1 == 204) {
          promise.complete();
          return;
        } else if (status1 != 201) {
          promise.fail("tenant post returned " + status1 + " " + res1.result().bodyAsString());
          return;
        }
        String id = res1.result().bodyAsJsonObject().getString("id");
        client.getTenantByOperationId(id, waitMs, res2 -> {
          if (res2.failed()) {
            promise.fail(res2.cause());
            return;
          }
          int status2 = res2.result().statusCode();
          if (status2 != 200) {
            promise.fail("tenant get returned " + status2 + " " + res2.result().bodyAsString());
            return;
          }
          JsonObject bodyJson = res2.result().bodyAsJsonObject();
          if (!bodyJson.getBoolean("complete")) {
            promise.fail("tenant job did not complete");
            return;
          }
          String error = bodyJson.getString("error");
          if (error != null) {
            promise.fail(error);
            return;
          }
          promise.complete();
        });
      });
    } catch (Exception e) {
      log.warn(e.getMessage(), e);
      promise.fail(e);
    }
    return promise.future();
  }
}
