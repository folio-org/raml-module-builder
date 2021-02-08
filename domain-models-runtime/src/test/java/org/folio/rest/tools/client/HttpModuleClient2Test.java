package org.folio.rest.tools.client;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@RunWith(VertxUnitRunner.class)
public class HttpModuleClient2Test {

  public class MyPojo {
    public String member;
  }

  Vertx vertx;

  int port1;
  int port2;

  String lastPath;
  Buffer lastBuffer;
  MultiMap lastHeaders;

  private void myPreHandle(RoutingContext ctx) {
    lastBuffer = Buffer.buffer();
    lastPath = ctx.request().path();
    lastHeaders = ctx.request().headers();
    if ("/test-error".equals(ctx.request().path())) {
      ctx.response().setStatusCode(400);
    } else {
      ctx.response().setStatusCode(200);
    }
    ctx.response().putHeader("Content-Type", "text-plain");
    ctx.request().handler(lastBuffer::appendBuffer);
    ctx.request().endHandler(res -> {
      ctx.response().end(lastBuffer);
    });
  }

  private Future<Void> startServer() {

    Router router = Router.router(vertx);

    router.routeWithRegex("/test.*").handler(this::myPreHandle);

    Promise<Void> promise = Promise.promise();
    HttpServerOptions so = new HttpServerOptions().setHandle100ContinueAutomatically(true);
    vertx.createHttpServer(so)
        .requestHandler(router)
        .listen(port1, x -> promise.handle(x.mapEmpty()));
    return promise.future();

  }
  @Before
  public void before(TestContext context) {
    vertx = Vertx.vertx();
    port1 = NetworkUtils.nextFreePort();
    port2 = NetworkUtils.nextFreePort();
    Future<Void> future = startServer();
    future.onComplete(context.asyncAssertSuccess());
  }

  @After
  public void after() {
    vertx.close();
  }

  @Test
  public void testWithPort(TestContext context) throws Exception {
    HttpModuleClient2 httpModuleClient2 = new HttpModuleClient2("localhost", port1, "tenant");

    CompletableFuture<Response> cf = httpModuleClient2.request("/test");
    Response response = cf.get(5, TimeUnit.SECONDS);

    context.assertNull(response.error);
    context.assertNull(response.exception);
    context.assertEquals("/test", lastPath);
    context.assertEquals("", lastBuffer.toString());
    context.assertEquals("tenant", lastHeaders.get("x-okapi-tenant"));
  }

  @Test
  public void testWithAbs(TestContext context) throws Exception {
    HttpModuleClient2 httpModuleClient2 = new HttpModuleClient2("http://localhost:" + port1, "tenant");

    CompletableFuture<Response> cf = httpModuleClient2.request("/test1");
    Response response = cf.get(5, TimeUnit.SECONDS);

    context.assertNull(response.error);
    context.assertNull(response.exception);
    context.assertEquals("/test1", lastPath);
    context.assertEquals("", lastBuffer.toString());

    cf = httpModuleClient2.request("/test2");
    response = cf.get(5, TimeUnit.SECONDS);

    context.assertNull(response.error);
    context.assertNull(response.exception);
    context.assertEquals("/test2", lastPath);
    context.assertEquals("", lastBuffer.toString());

    httpModuleClient2.closeClient();
  }

  @Test
  public void testWithAutoClose(TestContext context) throws Exception {
    HttpModuleClient2 httpModuleClient2 = new HttpModuleClient2("http://localhost:" + port1, "tenant", true);

    CompletableFuture<Response> cf = httpModuleClient2.request("/test1");
    Response response = cf.get(5, TimeUnit.SECONDS);

    context.assertNull(response.error);
    context.assertNull(response.exception);
    context.assertEquals("/test1", lastPath);
    context.assertEquals("", lastBuffer.toString());

    cf = httpModuleClient2.request("/test2");
    response = cf.get(5, TimeUnit.SECONDS);

    context.assertTrue(response.error.getString("errorMessage").contains("Client is closed"), response.error.getString("errorMessage"));
  }

  @Test
  public void testNotFound(TestContext context) throws Exception {
    HttpModuleClient2 httpModuleClient2 = new HttpModuleClient2("http://localhost:" + port1, "tenant");

    CompletableFuture<Response> cf = httpModuleClient2.request("/badpath");
    Response response = cf.get(5, TimeUnit.SECONDS);

    context.assertEquals(404, response.error.getInteger("statusCode"));
    context.assertTrue(response.error.getString("errorMessage").contains("Resource not found"), response.error.getString("errorMessage"));
    context.assertNull(response.exception);
    httpModuleClient2.closeClient();
  }

  @Test
  public void testEmptyError(TestContext context) throws Exception {
    HttpModuleClient2 httpModuleClient2 = new HttpModuleClient2("http://localhost:" + port1, "tenant");

    CompletableFuture<Response> cf = httpModuleClient2.request("/test-error");
    Response response = cf.get(5, TimeUnit.SECONDS);

    context.assertEquals(400, response.error.getInteger("statusCode"));
    context.assertNull(response.exception);
    httpModuleClient2.closeClient();
  }


  @Test
  public void testWithPojo(TestContext context) throws Exception {
    HttpModuleClient2 httpModuleClient2 = new HttpModuleClient2("localhost", port1, "tenant");

    MyPojo myPojo = new MyPojo();
    myPojo.member = "abc";

    Map<String,String> headers = new HashMap<>();
    headers.put("X-Name", "x-value");

    CompletableFuture<Response> cf = httpModuleClient2.request(HttpMethod.POST, myPojo, "/test-pojo", headers);
    Response response = cf.get(5, TimeUnit.SECONDS);

    context.assertNull(response.error);
    context.assertNull(response.exception);
    context.assertEquals("/test-pojo", lastPath);
    context.assertEquals("{\"member\":\"abc\"}", lastBuffer.toString());
    context.assertEquals("x-value", lastHeaders.get("x-name"));
  }

  @Test(expected = ExecutionException.class)
  public void testWithBadPojo(TestContext context) throws Exception {
    HttpModuleClient2 httpModuleClient2 = new HttpModuleClient2("localhost", port1, "tenant");

    Buffer badPojo = Buffer.buffer("{");

    Map<String,String> headers = new HashMap<>();
    headers.put("X-Name", "x-value");

    final CompletableFuture<Response> cf = httpModuleClient2.request(HttpMethod.POST, badPojo, "/test-pojo", headers);
    cf.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testWithBadPort(TestContext context) throws Exception {
    HttpModuleClient2 httpModuleClient2 = new HttpModuleClient2("localhost", port2, "tenant");

    CompletableFuture<Response> cf = httpModuleClient2.request("/test");
    Response response = cf.get(5, TimeUnit.SECONDS);
    // don't compare against locale dependent error message
    assertThat(response.getError().encodePrettily(), containsString("" + port2));
  }
}
