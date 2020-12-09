package org.folio.rest.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.folio.rest.resource.DomainModelConsts;
import org.folio.rest.tools.utils.VertxUtils;
import org.folio.util.ResourceUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class RamlsAPIIT {

  private static Vertx vertx;

  @Rule
  public Timeout rule = Timeout.seconds(20);

  @BeforeClass
  public static void setUpClass() {
    vertx = VertxUtils.getVertxWithExceptionHandler();
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void testGetRaml(TestContext context) {
    Async async = context.async();
    vertx.runOnContext(run -> {
      try {
        RamlsAPI ramlsAPI = new RamlsAPI();
        Map<String,String> map = new HashMap<>();
        ramlsAPI.getRamls(null, map, handle -> {
          context.assertTrue(handle.succeeded());
          context.assertEquals(200, handle.result().getStatus());
          context.assertEquals("application/json", handle.result().getHeaderString("Content-Type"));
          async.complete();
        }, Vertx.currentContext());
      } catch (Exception e) {
        context.fail(e);
      }
    });
    async.awaitSuccess();
  }

  @Test
  public void testGetRamlByPath(TestContext context) {
    Async async = context.async();
    vertx.runOnContext(run -> {
      try {
        RamlsAPI ramlsAPI = new RamlsAPI();
        Map<String,String> map = new HashMap<>();
        ramlsAPI.getRamls("test.raml", map, handle -> {
          context.assertTrue(handle.succeeded());
          context.assertEquals(200, handle.result().getStatus());
          context.assertEquals("application/raml+yaml", handle.result().getHeaderString("Content-Type"));
          async.complete();
        }, Vertx.currentContext());
      } catch (Exception e) {
        context.fail(e);
      }
    });
    async.awaitSuccess();
  }

  @Test
  public void testGetRamlByPathNotFound(TestContext context) {
    Async async = context.async();
    vertx.runOnContext(run -> {
      try {
        RamlsAPI ramlsAPI = new RamlsAPI();
        Map<String,String> map = new HashMap<>();
        ramlsAPI.getRamls("phantom.raml", map, handle -> {
          context.assertTrue(handle.succeeded());
          context.assertEquals(404, handle.result().getStatus());
          context.assertEquals("text/plain", handle.result().getHeaderString("Content-Type"));
          async.complete();
        }, Vertx.currentContext());
      } catch (Exception e) {
        context.fail(e);
      }
    });
    async.awaitSuccess();
  }

  // test.raml is a result from a unit test in domain-models-interface-extensions
  @Test
  public void testReplaceReferences(TestContext context) throws IOException {
    RamlsAPI ramlsAPI = new RamlsAPI();
    String raml = ResourceUtil.asString(System.getProperty("raml_files", DomainModelConsts.SOURCES_DEFAULT) + "/test.raml");
    raml = ramlsAPI.replaceReferences(raml, "http://localhost:9130");
    assertTrue(raml.contains("test: !include http://localhost:9130/_/jsonSchemas?path=test.schema"));
    assertFalse(raml.contains("test: !include test.schema"));
  }

}
