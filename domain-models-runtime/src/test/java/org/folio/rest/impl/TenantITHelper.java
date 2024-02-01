package org.folio.rest.impl;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.folio.dbschema.Schema;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.jaxrs.model.Book;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.TenantJob;
import org.folio.rest.persist.PgUtil;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientHelper;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.tools.utils.VertxUtils;
import org.hamcrest.CoreMatchers;
import org.junit.*;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import freemarker.template.TemplateException;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Row;

public class TenantITHelper {

  private static final int TIMER_WAIT = 10000;

  public static Future<Response> purgeTenant(Vertx vertx, String tenantId) {
    Map<String,String> headers = tenantId == null ? Map.of() : Map.of("X-Okapi-Tenant", tenantId);
    TenantAttributes attributes = new TenantAttributes().withPurge(true);
    return Future.future(promise -> new TenantAPI()
        .postTenantSync(attributes, headers, promise, vertx.getOrCreateContext()));
  }

  public static Future<Row> assertCount(Vertx vertx, TestContext context, String tenant, int expectedCount) {
    // TODO When executed in parallel on the second time, when the connection limit > 1, the relation doesn't exist.
    return PostgresClient.getInstance(vertx, tenant).selectSingle("SELECT count(*) from test_tenantapi")
        .onComplete(context.asyncAssertSuccess(row -> assertThat(row.getInteger(0), is(expectedCount))));
  }


  public static Future<Void> tenantPurge(Vertx vertx, TestContext context, String tenant) {
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
  public static CompositeFuture assertCountFour(Vertx vertx, TestContext context, String tenant, int expectedCount) {
    return GenericCompositeFuture.all(List.of(
        assertCount(vertx, context, tenant, expectedCount),
        assertCount(vertx, context, tenant, expectedCount),
        assertCount(vertx, context, tenant, expectedCount),
        assertCount(vertx, context, tenant, expectedCount)));
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

  public static String tenantPost(Vertx vertx, TenantAPI api, TestContext context, TenantAttributes tenantAttributes, String tenant) {
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
