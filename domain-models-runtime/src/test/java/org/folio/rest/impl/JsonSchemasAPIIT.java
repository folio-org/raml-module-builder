package org.folio.rest.impl;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.folio.rest.tools.utils.VertxUtils;
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
public class JsonSchemasAPIIT {

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
  public void testGetJsonSchema(TestContext context) {
    Async async = context.async();
    vertx.runOnContext(run -> {
      try {
        JsonSchemasAPI jsonSchemasAPI = new JsonSchemasAPI();
        Map<String,String> map = new HashMap<>();
        jsonSchemasAPI.getJsonSchemas(null, map, handle -> {
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
  public void testGetJsonSchemaByPath(TestContext context) {
    Async async = context.async();
    vertx.runOnContext(run -> {
      try {
        JsonSchemasAPI jsonSchemasAPI = new JsonSchemasAPI();
        Map<String,String> map = new HashMap<>();
        jsonSchemasAPI.getJsonSchemas("test.schema", map, handle -> {
          context.assertTrue(handle.succeeded());
          context.assertEquals(200, handle.result().getStatus());
          context.assertEquals("application/schema+json", handle.result().getHeaderString("Content-Type"));
          async.complete();
        }, Vertx.currentContext());
      } catch (Exception e) {
        context.fail(e);
      }
    });
    async.awaitSuccess();
  }

  @Test
  public void testGetJsonSchemaByPathNotFound(TestContext context) {
    Async async = context.async();
    vertx.runOnContext(run -> {
      try {
        JsonSchemasAPI jsonSchemasAPI = new JsonSchemasAPI();
        Map<String,String> map = new HashMap<>();
        jsonSchemasAPI.getJsonSchemas("phantom.schema", map, handle -> {
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

  private void testReplaceReferences(String jsonSchema) throws IOException {
    assertThat(JsonSchemasAPI.replaceReferences(jsonSchema, "http://localhost:9130"),
        containsString("\"http://localhost:9130/_/jsonSchemas?path=object.json\""));
  }

  @Test
  public void testReplaceReferencesUnix() throws IOException {
    testReplaceReferences(
        "{ \"$ref\" : \"file:C:%5CUsers%5Citsme%5Cmod-users%5Ctarget%5Cclasses%5Cramls%5Cobject.json\" }");
  }

  @Test
  public void testReplaceReferencesWindows() throws IOException {
    testReplaceReferences("{ \"$ref\" : \"file:/home/itsme/mod-users/target/classes/ramls/object.json\" }");
  }

}
