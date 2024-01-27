package org.folio.rest.impl;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.startsWith;
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

@RunWith(VertxUnitRunner.class)
public class TenantSharedPoolAPIIT {
  private static final String tenantId = "folio_shared";
  protected static Vertx vertx;
  private static Map<String,String> okapiHeaders = new HashMap<>();
  private static final int TIMER_WAIT = 10000;

  @Rule
  public Timeout rule = Timeout.seconds(1000);

  @BeforeClass
  public static void setUpClass() {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    purgeTenant("tenant1")
        .compose(x -> purgeTenant("tenant2"))
        .compose(x -> purgeTenant("tenant3"))
        .compose(x -> purgeTenant("tenant4"))
        .compose(x -> vertx.close())
        .onComplete(context.asyncAssertSuccess())
        .onComplete(x -> PostgresClientHelper.setSharedPgPool(false));
  }

  @After
  public void after(TestContext context) {
    purgeTenant(null).onComplete(context.asyncAssertSuccess());
  }

  private static Future<Response> purgeTenant(String tenantId) {
    Map<String,String> headers = tenantId == null ? Map.of() : Map.of("X-Okapi-Tenant", tenantId);
    TenantAttributes attributes = new TenantAttributes().withPurge(true);
    return Future.future(promise -> new TenantAPI()
        .postTenantSync(attributes, headers, promise, vertx.getOrCreateContext()));
  }

  private String tenantPost(TenantAPI api, TestContext context, TenantAttributes tenantAttributes) {
    return tenantPost(api, context, tenantAttributes, tenantId);
  }

  private String tenantPost(TenantAPI api, TestContext context, TenantAttributes tenantAttributes, String tenant) {
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

  private Future<Void> tenantPurge(TestContext context, String tenant) {
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



  private Future<Row> assertCount(TestContext context, String tenant, int expectedCount) {
    // TODO When executed in parallel on the second time, when the connection limit > 1, the relation doesn't exist.
    return PostgresClient.getInstance(vertx, tenant).selectSingle("SELECT count(*) from test_tenantapi")
        .onComplete(context.asyncAssertSuccess(row -> assertThat(row.getInteger(0), is(expectedCount))));
  }

  /**
   * Four parallel checks inspect four connections (idle or newly created).
   */
  private CompositeFuture assertCountFour(TestContext context, String tenant, int expectedCount) {
    return GenericCompositeFuture.all(List.of(
        assertCount(context, tenant, expectedCount),
        assertCount(context, tenant, expectedCount),
        assertCount(context, tenant, expectedCount),
        assertCount(context, tenant, expectedCount)));
  }

  private void assertTenantPurge(TestContext context, String tenant1, String tenant2, boolean sharedPool) {
    PostgresClient.closeAllClients();
    tenantPost(new TenantAPI(), context, null, tenant1);
    tenantPost(new TenantAPI(), context, null, tenant2);
    PostgresClient.getInstance(vertx, tenant1).execute("INSERT INTO test_tenantapi VALUES ('27f0857b-3165-4d5a-af77-229e4ad7921d', '{}')")
        .compose(x -> assertCountFour(context, tenant1, 1))
        .compose(x -> assertCountFour(context, tenant2, 0))
        .compose(x -> tenantPurge(context, tenant2))
        .compose(x -> assertCountFour(context, tenant1, 1))
        .onComplete(x -> {
          if (sharedPool) {
            PostgresClientHelper.setSharedPgPool(false);
          }
          context.asyncAssertSuccess();
        });
  }

  @Test
  public void postTenantPurgeSharedPool(TestContext context) {
    PostgresClientHelper.setSharedPgPool(true);
    assertTenantPurge(context, "tenant1", "tenant2", true);
  }

  @Test
  public void postTenantPoolShared(TestContext context) {
    PostgresClientHelper.setSharedPgPool(true);
    //PostgresClient.closeAllClients();

    var tenant = "tenant6";
    tenantPost(new TenantAPI(), context, null, tenant);
    PostgresClient.getInstance(vertx, tenant).execute("INSERT INTO test_tenantapi VALUES ('27f0857b-3165-4d5a-af77-229e4ad7921d', '{}')")
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .onComplete(x -> {
          PostgresClientHelper.setSharedPgPool(false);
          context.asyncAssertSuccess();
        });
  }
}
