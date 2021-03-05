package org.folio.rest.tools.utils;

import static org.hamcrest.Matchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.net.ConnectException;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.testing.UtilityClassTester;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class TenantInitTest {
  static Vertx vertx;
  static int port;
  static int postStatus = 204;
  static Buffer postResponse;
  static String postContent;

  static int getStatus = 204;
  static Buffer getResponse;
  static String getContent;
  static TenantClient client;

  private static void postHandler(RoutingContext ctx) {
    if (postContent != null) {
      ctx.response().putHeader("Content-Type", postContent);
    }
    ctx.response().setStatusCode(postStatus);
    if (postResponse == null) {
      ctx.end();
    } else {
      ctx.end(postResponse);
    }
  }

  private static void getHandler(RoutingContext ctx) {
    if (getContent != null) {
      ctx.response().putHeader("Content-Type", getContent);
    }
    ctx.response().setStatusCode(getStatus);
    if (getResponse == null) {
      ctx.end();
    } else {
      ctx.end(getResponse);
    }
  }

  private static void deleteHandler(RoutingContext ctx) {
    ctx.end();
  }

  @BeforeClass
  public static void beforeClass(TestContext context) {
    vertx = Vertx.vertx();
    port = NetworkUtils.nextFreePort();
    Router router = Router.router(vertx);
    router.post("/_/tenant").handler(BodyHandler.create());
    router.post("/_/tenant").handler(TenantInitTest::postHandler);
    router.getWithRegex("/_/tenant/.*").handler(TenantInitTest::getHandler);
    router.deleteWithRegex("/_/tenant/.*").handler(TenantInitTest::deleteHandler);

    client = new TenantClient("http://localhost:" + port, "testlib", null);
    HttpServerOptions so = new HttpServerOptions().setHandle100ContinueAutomatically(true);
    vertx.createHttpServer(so).requestHandler(router).listen(port).onComplete(context.asyncAssertSuccess());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void utilityClass() {
    UtilityClassTester.assertUtilityClass(TenantInit.class);
  }

  @Test
  public void testBadPortPost(TestContext context) {
    TenantClient client2 = new TenantClient("http://localhost:" + NetworkUtils.nextFreePort(), "library", null);
    TenantAttributes ta = new TenantAttributes().withModuleTo("module-1.0.0");
    TenantInit.exec(client2, ta, 1).onComplete(context.asyncAssertFailure(cause ->
      assertThat(cause, isA(ConnectException.class))
    ));
  }

  @Test
  public void testBadPortGet(TestContext context) {
    TenantClient client2 = new TenantClient("http://localhost:" + NetworkUtils.nextFreePort(), "library", null);
    Future.<Void>future(promise -> TenantInit.execGet(client2, "1", 1, promise))
    .onComplete(context.asyncAssertFailure(cause -> {
      assertThat(cause, isA(ConnectException.class));
    }));
  }

  @Test
  public void testStatus204(TestContext context) {
    TenantAttributes ta = new TenantAttributes().withModuleTo("module-1.0.0");
    postStatus = 204;
    postContent = null;
    postResponse = null;

    TenantInit.exec(client, ta, 1).onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testStatus400(TestContext context) {
    TenantAttributes ta = new TenantAttributes().withModuleTo("module-1.0.0");
    postStatus = 400;
    postContent = "text/plain";
    postResponse = Buffer.buffer("my error");

    TenantInit.exec(client, ta, 1).onComplete(context.asyncAssertFailure(cause ->
      context.assertEquals("tenant post returned 400 my error", cause.getMessage())
    ));
  }

  @Test
  public void testStatus201BadJson(TestContext context) {
    TenantAttributes ta = new TenantAttributes().withModuleTo("module-1.0.0");
    postStatus = 201;
    postContent = "application/json";
    postResponse = Buffer.buffer("{");

    TenantInit.exec(client, ta, 1).onComplete(context.asyncAssertFailure(cause ->
      context.assertTrue(cause.getMessage().startsWith("Failed to decode:"), cause.getMessage())
    ));
  }

  @Test
  public void testStatus201WrongType(TestContext context) {
    TenantAttributes ta = new TenantAttributes().withModuleTo("module-1.0.0");
    postStatus = 201;
    postContent = "application/json";
    postResponse = Buffer.buffer("{\"other\":1}");

    getStatus = 200;
    getContent = "application/json";
    getResponse = Buffer.buffer("{\"other\":1}");

    TenantInit.exec(client, ta, 1).onComplete(context.asyncAssertFailure(cause ->
      context.assertEquals("tenant job did not complete", cause.getMessage())
    ));
  }

  @Test
  public void testStatus400Get(TestContext context) {
    TenantAttributes ta = new TenantAttributes().withModuleTo("module-1.0.0");
    postStatus = 201;
    postContent = "application/json";
    postResponse = Buffer.buffer("{\"other\":1}");

    getStatus = 400;
    getContent = "text/plain";
    getResponse = Buffer.buffer("error in get");

    TenantInit.exec(client, ta, 1).onComplete(context.asyncAssertFailure(cause ->
      context.assertEquals("tenant get returned 400 error in get", cause.getMessage())
    ));
  }

  @Test
  public void testStatusJobBadJson(TestContext context) {
    TenantAttributes ta = new TenantAttributes().withModuleTo("module-1.0.0");
    postStatus = 201;
    postContent = "application/json";
    postResponse = Buffer.buffer("{\"id\":1}");

    getStatus = 200;
    getContent = "application/json";
    getResponse = Buffer.buffer("{");

    TenantInit.exec(client, ta, 1).onComplete(context.asyncAssertFailure(cause ->
      context.assertTrue(cause.getMessage().startsWith("Failed to decode:"), cause.getMessage())
    ));
  }

  @Test
  public void testStatusJobError(TestContext context) {
    TenantAttributes ta = new TenantAttributes().withModuleTo("module-1.0.0");
    postStatus = 201;
    postContent = "application/json";
    postResponse = Buffer.buffer("{\"id\":1}");

    getStatus = 200;
    getContent = "application/json";
    getResponse = Buffer.buffer("{\"id\":1,\"complete\":true, \"error\":\"job error\"}");

    TenantInit.exec(client, ta, 1).onComplete(context.asyncAssertFailure(cause ->
      context.assertEquals("job error", cause.getMessage())
    ));
  }

  @Test
  public void testStatusJobOK(TestContext context) {
    TenantAttributes ta = new TenantAttributes().withModuleTo("module-1.0.0");
    postStatus = 201;
    postContent = "application/json";
    postResponse = Buffer.buffer("{\"id\":1}");

    getStatus = 200;
    getContent = "application/json";
    getResponse = Buffer.buffer("{\"id\":1,\"complete\":true}");

    TenantInit.exec(client, ta, 1).onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testPurge(TestContext context) {
    postStatus = 201;
    postContent = "application/json";
    postResponse = Buffer.buffer("{\"id\":1}");

    getStatus = 200;
    getContent = "application/json";
    getResponse = Buffer.buffer("{\"id\":1,\"complete\":true}");

    TenantInit.purge(client, 1).onComplete(context.asyncAssertSuccess());
  }

}
