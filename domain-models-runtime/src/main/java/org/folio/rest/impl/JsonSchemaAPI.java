package org.folio.rest.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;
import java.util.jar.JarEntry;
import java.util.Enumeration;

import javax.ws.rs.core.Response;

import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.JsonSchema;
import org.folio.rest.tools.utils.ObjectMapperTool;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSchemaAPI implements JsonSchema {

  private static final Logger log = LoggerFactory.getLogger(JsonSchemaAPI.class);

  @Validate
  @Override
  public void getJsonSchema(
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext
  ) {
    vertxContext.runOnContext(v -> {
      try {
        List<String> schemas = getSchemas();
        asyncResultHandler.handle(
          Future.succeededFuture(
            GetJsonSchemaResponse.respond200WithApplicationJson(schemas)
          )
        );
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        asyncResultHandler.handle(
          Future.succeededFuture(
            GetJsonSchemaResponse.respond500WithTextPlain(e.getMessage())
          )
        );
      }
    });
  }

  @Validate
  @Override
  public void getJsonSchemaByName(
    String name,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext
  ) {
    vertxContext.runOnContext(v -> {
      try {
        JsonNode schema = getSchemaByName(name);
        if(schema != null) {
          asyncResultHandler.handle(
            Future.succeededFuture(
              GetJsonSchemaByNameResponse.respond200WithApplicationJson(schema)
            )
          );
        } else {
          String notFoundMessage = "Schema " + name + " not found";
          asyncResultHandler.handle(
            Future.succeededFuture(
              GetJsonSchemaByNameResponse.respond404WithTextPlain(notFoundMessage)
            )
          );
        }
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        asyncResultHandler.handle(
          Future.succeededFuture(
            GetJsonSchemaByNameResponse.respond500WithTextPlain(e.getMessage())
          )
        );
      }
    });
  }

  private List<String> getSchemas() throws IOException {
    List<String> schemas = new ArrayList<>();
    File jarFile = new File(getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
    JarFile jar = new JarFile(jarFile);
    Enumeration<JarEntry> entries = jar.entries();
    while(entries.hasMoreElements()) {
      JarEntry entry = entries.nextElement();
      String entryName = entry.getName();
      if (entryName.startsWith("ramls") && !entryName.startsWith("ramls/raml-util")) {
        String schemaName = entryName.substring(entryName.lastIndexOf("/") + 1);
        if(schemaName.endsWith(".json") || schemaName.endsWith(".schema")) {
          try {
            InputStream is = jar.getInputStream(entry);
            ObjectMapperTool.getMapper().readValue(is, JsonNode.class);
            is.close();
            schemas.add(schemaName);
          } catch(Exception e) {
            log.info("{} is not a valid json file", entryName);
          }
        }
      }
    }
    jar.close();
    return schemas;
  }

  private JsonNode getSchemaByName(String name) throws IOException {
    JsonNode schema = null;
    File jarFile = new File(getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
    JarFile jar = new JarFile(jarFile);
    Enumeration<JarEntry> entries = jar.entries();
    while(entries.hasMoreElements()) {
      JarEntry entry = entries.nextElement();
      String entryName = entry.getName();
      if (entryName.startsWith("ramls") && !entryName.startsWith("ramls/raml-util")) {
        String schemaName = entryName.substring(entryName.lastIndexOf("/") + 1);
        if((schemaName.endsWith(".json") || schemaName.endsWith(".schema")) && schemaName.equals(name)) {
          try {
            InputStream is = jar.getInputStream(entry);
            schema = ObjectMapperTool.getMapper().readValue(is, JsonNode.class);
            is.close();
            break;
          } catch(Exception e) {
            log.info("{} is not a valid json file", entryName);
          }
        }
      }
    }
    jar.close();
    return schema;
  }

}
