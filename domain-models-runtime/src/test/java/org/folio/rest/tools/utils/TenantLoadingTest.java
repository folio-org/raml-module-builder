package org.folio.rest.tools.utils;

import java.util.List;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.TenantAttributes;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class TenantLoadingTest {

  Vertx vertx;
  static int port = 0;
  int putStatus; // for our fake server
  int postStatus; // for our fake server

  Set<String> ids = new HashSet<>();

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4j2LogDelegateFactory");
    port = NetworkUtils.nextFreePort();
  }

  private void fakeHttpServerHandler(RoutingContext ctx) {
    ctx.response().setChunked(true);
    Buffer buffer = Buffer.buffer();
    ctx.request().handler(buffer::appendBuffer);
    ctx.request().endHandler(x -> {
      if (ctx.request().method() == HttpMethod.PUT) {
        String path = ctx.request().path();
        int idx = path.lastIndexOf('/');
        if (idx != -1) {
          ids.add(path.substring(idx + 1));
          System.out.println("adding " + path.substring(idx + 1));
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
    Async async = context.async();
    Router router = Router.router(vertx);
    router.post("/data").handler(this::fakeHttpServerHandler);
    router.putWithRegex("/data/.*").handler(this::fakeHttpServerHandler);
    putStatus = 200;
    postStatus = 201;
    ids.clear();
    HttpServerOptions so = new HttpServerOptions().setHandle100ContinueAutomatically(true);
    vertx.createHttpServer(so)
      .requestHandler(router::accept)
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
    Async async = context.async();
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));
    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.succeeded());
      context.assertEquals(2, res.result());
      context.assertTrue(ids.contains("1"));
      context.assertTrue(ids.contains("2"));
      async.complete();
    });
  }

  @Test
  public void testOKDeleteStatus204(TestContext context) {
    Async async = context.async();
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
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.succeeded());
      context.assertEquals(2, res.result());
      context.assertTrue(ids.contains("1"));
      context.assertTrue(ids.contains("2"));
      async.complete();
    });
  }

  @Test
  public void testNoOkapiUrlTo(TestContext context) {
    Async async = context.async();
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.failed());
      context.assertEquals("No X-Okapi-Url-to header", res.cause().getLocalizedMessage());
      async.complete();
    });
  }

  @Test
  public void testBadOkapiUrlTo(TestContext context) {
    Async async = context.async();
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port + 1));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.failed());
      async.complete();
    });
  }

  @Test
  public void testPutFailure(TestContext context) {
    Async async = context.async();
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
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.failed());
      async.complete();
    });
  }

  @Test
  public void testPostOk(TestContext context) {
    Async async = context.async();
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
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.succeeded());
      context.assertEquals(2, res.result());
      context.assertTrue(ids.contains("1"));
      context.assertTrue(ids.contains("2"));
      async.complete();
    });
  }

  @Test
  public void testPostOk404(TestContext context) {
    Async async = context.async();
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
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.succeeded());
      context.assertEquals(2, res.result());
      context.assertTrue(ids.contains("1"));
      context.assertTrue(ids.contains("2"));
      async.complete();
    });
  }

  @Test
  public void testPostOk400(TestContext context) {
    Async async = context.async();
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
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.succeeded());
      context.assertEquals(2, res.result());
      context.assertTrue(ids.contains("1"));
      context.assertTrue(ids.contains("2"));
      async.complete();
    });
  }



  @Test
  public void testBadUriPath(TestContext context) {
    Async async = context.async();
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data1");
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.failed());
      async.complete();
    });
  }

  @Test
  public void testDataPathDoesNotExist(TestContext context) {
    Async async = context.async();
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data1", "data");
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.succeeded());
      context.assertEquals(0, res.result());
      async.complete();
    });
  }

  @Test
  public void testDataPathDoesNotExist2(TestContext context) {
    Async async = context.async();
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-none", "data", "data");
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.succeeded());
      context.assertEquals(0, res.result());
      async.complete();
    });
  }

  @Test
  public void testSkip(TestContext context) {
    Async async = context.async();
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("false"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.succeeded());
      context.assertEquals(0, res.result());
      async.complete();
    });
  }

  @Test
  public void testSkip2(TestContext context) {
    Async async = context.async();
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadSample").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data", "data");
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.succeeded());
      context.assertEquals(0, res.result());
      async.complete();
    });
  }

  @Test
  public void testFailNoIdInData(TestContext context) {
    Async async = context.async();
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));

    TenantLoading tl = new TenantLoading();
    tl.addJsonIdContent("loadRef", "tenant-load-ref", "data-w-id", "data");
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.failed());
      async.complete();
    });
  }

  @Test
  public void testOKIdBasename(TestContext context) {
    Async async = context.async();
    List<Parameter> parameters = new LinkedList<>();
    parameters.add(new Parameter().withKey("loadRef").withValue("true"));
    TenantAttributes tenantAttributes = new TenantAttributes()
      .withModuleTo("mod-1.0.0")
      .withParameters(parameters);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("X-Okapi-Url-to", "http://localhost:" + Integer.toString(port));
    TenantLoading tl = new TenantLoading();
    tl.addJsonIdBasename("loadRef", "tenant-load-ref", "data-w-id", "data/%d");
    tl.perform(tenantAttributes, headers, vertx, res -> {
      context.assertTrue(res.succeeded());
      context.assertEquals(1, res.result());
      context.assertTrue(ids.contains("1"));
      async.complete();
    });
  }

  @Test
  public void testFilesfromExistingJar(TestContext context) {
    List<URL> urls = new LinkedList<>();
    InputStream s = null;
    try {
      // load resources from "overrides" in domain-models-interface-extensions
      urls = TenantLoading.getURLsFromClassPathDir("overrides");
      if (!urls.isEmpty()) {
        URL url = urls.get(0);
        if (url != null) {
          s = url.openStream();
        }
        if (s != null) {
          s.close();
        }
      }
    } catch (IOException ex) {

    } catch (URISyntaxException ex) {

    }
    context.assertFalse(urls.isEmpty());
    context.assertNotNull(s);
  }
}
