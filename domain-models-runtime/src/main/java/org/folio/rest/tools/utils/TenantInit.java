package org.folio.rest.tools.utils;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
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
      log.info("about to call POST /_/tenant");
      client.postTenant(tenantAttributes, res1 -> {
        if (res1.failed()) {
          promise.fail(res1.cause());
          return;
        }
        int status = res1.result().statusCode();
        if (status == 204) {
          promise.complete();
          return;
        } else if (status != 201) {
          promise.fail("tenant post returned " + status + " " + res1.result().bodyAsString());
          return;
        }
        String id = res1.result().bodyAsJsonObject().getString("id");

        log.info("about to call GET /_/tenant/" + id);
        try {
          promise.handle(Future.<HttpResponse<Buffer>>future(p -> client.getTenantByOperationId(id, waitMs, p))
              .compose(res2 -> {
                log.info("GET returned");
                if (res2.statusCode() != 200) {
                  return Future.failedFuture("tenant get returned " + res2.statusCode() + " " + res2.bodyAsString());
                }
                JsonObject bodyJson = res2.bodyAsJsonObject();
                if (!bodyJson.getBoolean("complete")) {
                  return Future.failedFuture("tenant job did not complete");
                }
                String error = bodyJson.getString("error");
                if (error != null) {
                  return Future.failedFuture(error);
                }
                return Future.succeededFuture();
              }));
        } catch (Exception e) {
          log.warn(e.getMessage(), e);
          promise.fail(e);
        }
      });
    } catch (Exception e) {
      log.warn(e.getMessage(), e);
      promise.fail(e);
    }
    return promise.future();
  }
}
