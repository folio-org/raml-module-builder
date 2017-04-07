package org.folio.rest.impl;

import java.util.HashMap;
import java.util.Map;

import org.folio.rest.jaxrs.model.TenantAttributes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class TenantAPITest {
  private static final String tenantId = "folio_shared";
  protected static Vertx vertx;

  @BeforeClass
  public static void setUpClass() {
    vertx = Vertx.vertx();
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
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

  public void tenantDelete(TestContext context) {
    Async async = context.async();
    vertx.executeBlocking(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      try {
        Map<String,String> map = new HashMap<>();
        map.put("TenantId", tenantId);
        tenantAPI.deleteTenant(map, h -> {
          tenantAPI.tenantExists(Vertx.currentContext(), tenantId, onSuccess(context, bool -> {
            context.assertFalse(bool, "tenant exists");
            run.complete();
          }));
        }, Vertx.currentContext());
      } catch (Exception e) {
        context.fail(e);
      }
    }, end -> async.complete());
    async.await();
  }

  public void tenantPost(TestContext context) {
    Async async = context.async();
    vertx.executeBlocking(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      try {
        TenantAttributes tenantAttributes = new TenantAttributes();
        Map<String,String> map = new HashMap<>();
        map.put("TenantId", tenantId);
        tenantAPI.postTenant(tenantAttributes, map, h -> {
          tenantAPI.tenantExists(Vertx.currentContext(), tenantId, onSuccess(context, bool -> {
            context.assertTrue(bool, "tenant exists");
            run.complete();
          }));
        }, Vertx.currentContext());
      } catch (Exception e) {
        context.fail(e);
      }
    }, end -> async.complete());
    async.await();
  }

  @Test
  public void multi(TestContext context) {
    tenantDelete(context);  // make sure tenant does not exist
    tenantPost(context);    // create tenant
    tenantPost(context);    // create tenant when tenant already exists
    tenantDelete(context);  // delete existing tenant
    tenantDelete(context);  // delete non existing tenant
  }

  @Test
  public void invalidTenantName(TestContext context) {
    String invalidTenantId = "- ";
    Async async = context.async();
    vertx.executeBlocking(run -> {
      new TenantAPI().tenantExists(Vertx.currentContext(), invalidTenantId, h -> {
        context.assertTrue(h.succeeded());
        context.assertFalse(h.result());
        run.complete();
      });
    }, end -> async.complete());
  }
}
