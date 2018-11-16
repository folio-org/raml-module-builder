package org.folio.rest.impl;

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.IOUtils;
import org.folio.rest.tools.utils.VertxUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import io.vertx.core.Vertx;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class JsonSchemasAPIIT {

  private static Vertx vertx;

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4j2LogDelegateFactory");
  }

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

  @Test
  public void testReplaceReferences(TestContext context) throws IOException {
    JsonSchemasAPI jsonSchemasAPI = new JsonSchemasAPI();
    ObjectMapper mapper = new ObjectMapper();
    URL jsonSchemaUrl = getClass().getClassLoader().getResource("ramls/test.schema");
    String jsonSchema = IOUtils.toString(new FileInputStream(jsonSchemaUrl.getFile()), StandardCharsets.UTF_8.name());
    jsonSchema = jsonSchemasAPI.replaceReferences(jsonSchema, "http://localhost:9130");
    JsonNode testNode = mapper.readValue(jsonSchema, JsonNode.class);
    String objectsRef = testNode.get("properties").get("objects").get("items").get("$ref").asText();
    assertEquals("http://localhost:9130/_/jsonSchemas?path=object.json", objectsRef);
  }

}
