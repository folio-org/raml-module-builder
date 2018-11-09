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

import org.apache.commons.io.IOUtils;

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
      if (entryName.startsWith("ramls/") && entryName.endsWith(".json") && !entryName.startsWith("apidocs/")) {
        String schemaPath = entryName.substring(6);
        if(!schemaPath.contains("/")) {
          String schemaName = schemaPath.substring(schemaPath.lastIndexOf("/") + 1);
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
    log.info("\n\n\n\n{}", name);
    JsonNode schema = null;
    File jarFile = new File(getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
    JarFile jar = new JarFile(jarFile);
    Enumeration<JarEntry> entries = jar.entries();
    while(entries.hasMoreElements()) {
      JarEntry entry = entries.nextElement();
      String entryName = entry.getName();
      log.info("  {}", entryName);
      if (entryName.startsWith("ramls/")) {
        String schemaName = entryName.substring(entryName.lastIndexOf("/") + 1);
        log.info("    {}", schemaName);
        if(schemaName.equals(name)) {
          log.info("** match ** ");
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
    log.info("\n\n\n\n");
    return schema;
  }

}
