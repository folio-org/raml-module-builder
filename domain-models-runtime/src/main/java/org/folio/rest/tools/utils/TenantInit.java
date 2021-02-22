package org.folio.rest.tools.utils;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.TenantJob;

public final class TenantInit {
  private static final Logger log = LogManager.getLogger(TenantInit.class);

  private TenantInit() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Perform tenant purge, wait for result and delete job.
   * @param client Tenant Client.
   * @param waitMs number of milliseconds to wait for job completion (0=no wait).
   * @return async result.
   */
  public static Future<Void> purge(TenantClient client, int waitMs) {
    TenantAttributes at = new TenantAttributes().withPurge(true);
    return exec(client, at, waitMs);
  }

  /**
   * Perform tenant post, wait for result and delete job.
   * @param client Tenant Client.
   * @param tenantAttributes attributes for tenant post.
   * @param waitMs number of milliseconds to wait for job completion (0=no wait).
   * @return async result.
   */
  public static Future<Void> exec(TenantClient client, TenantAttributes tenantAttributes, int waitMs) {
    Promise<Void> promise = Promise.promise();
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
      try {
        TenantJob job = res1.result().bodyAsJson(TenantJob.class);
        execGet(client, job.getId(), waitMs, promise);
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        promise.fail(e);
      }
    });
    return promise.future();
  }

  static void execGet(TenantClient client, String id, int waitMs, Promise<Void> promise) {
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
      try {
        TenantJob job2 = res2.result().bodyAsJson(TenantJob.class);
        client.deleteTenantByOperationId(id, res3 -> {
          // we don't care about errors returned by delete
          String error = job2.getError();
          if (!Boolean.TRUE.equals(job2.getComplete())) {
            promise.fail("tenant job did not complete");
          } else if (error != null) {
            promise.fail(error);
          } else {
            promise.complete();
          }
        });
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        promise.fail(e);
      }
    });
  }
}
