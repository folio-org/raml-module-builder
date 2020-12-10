package org.folio.rest.tools.utils;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.net.MalformedURLException;
import java.util.List;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class TenantLoadingTest {

  @Rule
  public Timeout timeout = Timeout.seconds(5);

  Vertx vertx;
  int port;
  int putStatus; // for our fake server
  int postStatus; // for our fake server

  Set<String> ids = new HashSet<>();

  private void fakeHttpServerHandler(RoutingContext ctx) {
    ctx.response().setChunked(true);
    Buffer buffer = Buffer.buffer();
    ctx.request().handler(buffer::appendBuffer);
    ctx.request().endHandler(x -> {
      if (ctx.request().method() == HttpMethod.PUT) {
        String path = ctx.request().path();
        int idx = path.lastIndexOf('/');
        if (idx != -1) {
          try {
            String id = URLDecoder.decode(path.substring(idx + 1), "UTF-8");
            ids.add(id);
          } catch (UnsupportedEncodingException ex) {
            ctx.response().setStatusCode(400);
            return;
          }
        }
        ctx.response().setStatusCode(putStatus);
      } else if (ctx.request().method() == HttpMethod.POST) {
        ctx.response().setStatusCode(postStatus);
      } else {
        ctx.response().setStatusCode(405);
      }
      ctx.response().end();
    });
  }

  @Before
  public void setUp(TestContext context) {
    vertx = Vertx.vertx();
    port = NetworkUtils.nextFreePort();;
    Async async = context.async();
    Router router = Router.router(vertx);
    router.post("/data").handler(this::fakeHttpServerHandler);
    router.putWithRegex("/data/.*").handler(this::fakeHttpServerHandler);
    putStatus = 200;
    postStatus = 201;
    ids.clear();
    HttpServerOptions so = new HttpServerOptions().setHandle100ContinueAutomatically(true);
    vertx.createHttpServer(so)
      .requestHandler(router)
      .listen(
        port,
        result -> {
          if (result.failed()) {
            context.fail(result.cause());
          }
          async.complete();
        }
      );
  }

  @After
  public void tearDown(TestContext context) {
    Async async = context.async();
    vertx.close(x -> async.complete());
  }

  @Test
  public void testOK(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));
    TenantLoading tl = new TenantLoading()
      .withKey("loadRef")
      .withLead("tenant-load-ref")
      .add("data");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess(res -> {
      context.assertEquals(2, res);
      context.assertTrue(ids.contains("1"));
      context.assertTrue(ids.contains("2"));
    }));
  }

  @Test
  public void testPerformFutureOK(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
        .withModuleTo("mod-1.0.0")
        .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));
    TenantLoading tl = new TenantLoading()
        .withKey("loadRef")
        .withLead("tenant-load-ref")
        .add("data");
    tl.perform(tenantAttributes, headers, vertx.getOrCreateContext(), 10)
        .onComplete(context.asyncAssertSuccess(cnt -> context.assertEquals(12, cnt)));
  }

  public String myFilter(String content) {
    JsonObject obj = new JsonObject(content);
    String id = obj.getString("id");
    obj.put("id", "X" + id);
    return obj.encodePrettily();
  }

  @Test
  public void testOKWithContentFilter(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));
    TenantLoading tl = new TenantLoading()
      .withKey("loadRef")
      .withLead("tenant-load-ref")
      .withFilter(this::myFilter)
      .add("data");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess(res -> {
      context.assertEquals(2, res);
      context.assertTrue(ids.contains("X1"));
      context.assertTrue(ids.contains("X2"));
    }));
  }

  @Test
  public void testOKContentIdName(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));
    TenantLoading tl = new TenantLoading()
      .withKey("loadRef")
      .withLead("tenant-load-ref")
      .withContent("name")
      .add("data-w-id", "data");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess(res -> {
      context.assertEquals(1, res);
      context.assertTrue(ids.contains("number 1"));
    }));
  }

  @Test
  public void testOKDeleteStatus204(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));
    putStatus = 204;
    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess(res -> {
      context.assertEquals(2, res);
      context.assertTrue(ids.contains("1"));
      context.assertTrue(ids.contains("2"));
    }));
  }

  @Test
  public void testOKNullTenantAttributes(TestContext context) {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));
    TenantLoading tl = new TenantLoading()
      .withKey("loadRef")
      .withLead("tenant-load-ref")
      .add("data");
    tl.perform(null, headers, vertx, context.asyncAssertSuccess(res -> {
      context.assertEquals(0, res);
    }));
  }

  @Test
  public void testNoOkapiUrlTo(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertFailure(cause ->
      context.assertEquals("No X-Okapi-Url header", cause.getMessage())
    ));
  }

  @Test
  public void testBadOkapiUrlTo(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port + 1));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertFailure());
  }

  @Test
  public void testPutFailure(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.withKey("loadRef").withLead("tenant-load-ref").withIdContent().add("data", "data");
    putStatus = 500;
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertFailure());
  }

  @Test
  public void testokWithAcceptStatus(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    putStatus = 500;
    tl.withAcceptStatus(500);
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess());
  }

  @Test
  public void testOkStatus422(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    putStatus = 422;
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess());
  }

  @Test
  public void testPostOk(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    putStatus = 404; // so that PUT will return 404 and we can POST
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess(res -> {
      context.assertEquals(2, res);
      context.assertTrue(ids.contains("1"));
      context.assertTrue(ids.contains("2"));
    }));
  }

  @Test
  public void testPostOk404(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    putStatus = 404; // so that PUT will return 404 and we can POST
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess(res -> {
      context.assertEquals(2, res);
      context.assertTrue(ids.contains("1"));
      context.assertTrue(ids.contains("2"));
    }));
  }

  @Test
  public void testPostOk400(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    putStatus = 400; // so that PUT will return 400 and we can POST
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess(res -> {
      context.assertEquals(2, res);
      context.assertTrue(ids.contains("1"));
      context.assertTrue(ids.contains("2"));
    }));
  }

  @Test
  public void testBadUriPath(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data1");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertFailure());
  }

  @Test
  public void testDataPathDoesNotExist(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data1", "data");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess(res -> {
      context.assertEquals(0, res);
    }));
  }

  @Test
  public void testDataPathDoesNotExist2(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-none", "data", "data");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess(res -> {
      context.assertEquals(0, res);
    }));
  }

  @Test
  public void testSkip(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("false"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess(res -> {
      context.assertEquals(0, res);
    }));
  }

  @Test
  public void testSkip2(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadSample").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess(res -> {
      context.assertEquals(0, res);
    }));
  }

  @Test
  public void testFailNoIdInData(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data-w-id", "data");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertFailure());
  }

  @Test
  public void testOKIdBasename(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));
    TenantLoading tl = new TenantLoading();
    tl.addJsonIdBasename("loadRef", "tenant-load-ref", "data-w-id", "data/%d");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess(res -> {
      context.assertEquals(1, res);
      context.assertTrue(ids.contains("1"));
    }));
  }

  @Test
  public void testOKPostOnly(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));
    TenantLoading tl = new TenantLoading().withKey("loadRef").withLead("tenant-load-ref");
    tl.withPostOnly().add("data");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess(res ->
      context.assertEquals(2, res)
    ));
  }

  @Test
  public void testOKIdRaw(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));
    TenantLoading tl = new TenantLoading().withKey("loadRef").withLead("tenant-load-ref");
    tl.withIdRaw().add("data-w-id", "data/1");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertSuccess(res -> {
      context.assertEquals(1, res);
      context.assertTrue(ids.contains("1"));
    }));
  }

  @Test
  public void test404IdRaw(TestContext context) {
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));
    TenantLoading tl = new TenantLoading().withKey("loadRef").withLead("tenant-load-ref");
    tl.withIdRaw().add("data-w-id", "data");
    tl.perform(tenantAttributes, headers, vertx, context.asyncAssertFailure());
  }

  private void assertGetIdBase(TestContext context, String path, String expectedBase) {
    TenantLoading.getIdBase(path).onComplete(context.asyncAssertSuccess(res ->
      context.assertEquals(expectedBase, res)
    ));
  }

  private void assertGetIdBaseFail(TestContext context, String path) {
    TenantLoading.getIdBase(path).onComplete(context.asyncAssertFailure(cause ->
        context.assertEquals("No basename for " + path, cause.getMessage())
    ));
  }

  @Test
  public void testGetIdBase(TestContext context) {
    assertGetIdBase(context, "/path/a.json", "/a");
    assertGetIdBase(context, "/path/a", "/a");
    assertGetIdBase(context, "/a.b/c", "/c");
    assertGetIdBaseFail(context, "a.json");
    assertGetIdBaseFail(context, "a");
  }

  @Test
  public void testFilesfromExistingJar(TestContext context) throws Exception {
    // load a resource like META-INF/vertx/vertx-version.txt that a vertx jar ships with
    List<URL> urls = TenantLoading.getURLsFromClassPathDir("META-INF/vertx");
    for (URL url : urls) {
      try (InputStream inputStream = url.openStream()) {
        if (inputStream.available() > 0) {
          return; // success
        }
        // otherwise this might be a directory, so try next
      }
    }
    context.fail("Cannot read a file from jar: " + urls);
  }

  @Test
  public void testGetContentFail(TestContext context) throws MalformedURLException {
    String filename = UUID.randomUUID().toString();
    TenantLoading.getContent(new URL("file:/" + filename), null)
        .onComplete(context.asyncAssertFailure(cause ->
            context.assertTrue(cause.getMessage().startsWith("IOException for url file:/" + filename),
                cause.getMessage())));
  }
}
