package org.folio.rest.impl;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.TenantJob;
import org.folio.rest.persist.PostgresClient;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.sqlclient.Row;

public class TenantITHelper {
  protected static final int TIMER_WAIT = 10000;
  protected static final String tenantId = "folio_shared";
  protected static Map<String,String> okapiHeaders = new HashMap<>();

  protected static Vertx vertx;

  protected static Future<Response> purgeTenant(String tenantId) {
    Map<String,String> headers = tenantId == null ? Map.of() : Map.of("X-Okapi-Tenant", tenantId);
    TenantAttributes attributes = new TenantAttributes().withPurge(true);
    return Future.future(promise -> new TenantAPI()
        .postTenantSync(attributes, headers, promise, vertx.getOrCreateContext()));
  }

  /**
   * Return a lambda that handles an AsyncResult this way: On success pass the result
   * to the handler, on failure pass the causing Throwable to testContext.
   * @param testContext - where to invoke fail(Throwable)
   * @param handler - where to inject the result
   * @return the lambda
   */
  protected static <T> Handler<AsyncResult<T>> onSuccess(TestContext testContext, Handler<T> handler) {
    return asyncResult -> {
      if (asyncResult.succeeded()) {
        handler.handle(asyncResult.result());
      } else {
        testContext.fail(asyncResult.cause());
      }
    };
  }

  protected Future<Row> assertCount(TestContext context, String tenant, int expectedCount) {
    // TODO When executed in parallel on the second time, when the connection limit > 1, the relation doesn't exist.
    return PostgresClient.getInstance(vertx, tenant).selectSingle("SELECT count(*) from test_tenantapi")
        .onComplete(context.asyncAssertSuccess(row -> assertThat(row.getInteger(0), is(expectedCount))));
  }


  protected Future<Void> tenantPurge(TestContext context, String tenant) {
    TenantAttributes tenantAttributes = new TenantAttributes();
    tenantAttributes.setPurge(true);
    TenantAPI tenantAPI = new TenantAPI();
    return Future.future(promise ->
        tenantAPI.postTenant(tenantAttributes, Map.of("X-Okapi-Tenant", tenant), onSuccess(context, res1 -> {
          context.assertEquals(204, res1.getStatus());
          tenantAPI.tenantExists(vertx.getOrCreateContext(), tenant)
              .onComplete(context.asyncAssertSuccess(bool -> {
                context.assertFalse(bool, "tenant exists after purge");
                promise.complete();
              }));
        }), vertx.getOrCreateContext()));
  }

  /**
   * Four parallel checks inspect four connections (idle or newly created).
   */
  protected CompositeFuture assertCountFour(TestContext context, String tenant, int expectedCount) {
    return GenericCompositeFuture.all(List.of(
        assertCount(context, tenant, expectedCount),
        assertCount(context, tenant, expectedCount),
        assertCount(context, tenant, expectedCount),
        assertCount(context, tenant, expectedCount)));
  }

  protected static String tenantPost(TenantAPI api, TestContext context, TenantAttributes tenantAttributes, String tenant) {
    Map<String,String> headers = Map.of("X-Okapi-Tenant", tenant);
    Async async = context.async();
    StringBuilder id = new StringBuilder();
    api.postTenant(tenantAttributes, headers, onSuccess(context, res1 -> {
      TenantJob job = (TenantJob) res1.getEntity();
      id.append(job.getId());
      api.getTenantByOperationId(job.getId(), TIMER_WAIT, headers, onSuccess(context, res2 -> {
        TenantJob o = (TenantJob) res2.getEntity();
        context.assertTrue(o.getComplete());
        api.tenantExists(Vertx.currentContext(), tenant)
            .onComplete(onSuccess(context, bool -> {
              context.assertTrue(bool, "tenant exists after post");
              async.complete();
            }));
      }), vertx.getOrCreateContext());
    }), vertx.getOrCreateContext());
    async.await();
    return id.toString();
  }
}
