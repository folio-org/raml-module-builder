package org.folio.rest.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.StringBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;
import java.util.jar.JarEntry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class JsonSchemaAPI implements JsonSchema {

  private static final Logger log = LoggerFactory.getLogger(JsonSchemaAPI.class);

  private static final Pattern REF_MATCH_PATTERN = Pattern.compile("\\\"\\$ref\\\"\\s*:\\s*\\\"(.*?)\\\"");

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
    String okapiUrl = okapiHeaders.get("x-okapi-url");
    vertxContext.runOnContext(v -> {
      try {
        String schema = getSchemaByName(name, okapiUrl);
        if(schema != null) {
          asyncResultHandler.handle(
            Future.succeededFuture(
              GetJsonSchemaByNameResponse.respond200WithApplicationSchemaJson(schema)
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
    List<JarEntry> entries = Collections.list(jar.entries());
    for (JarEntry entry : entries) {
      String entryName = entry.getName();
      if (entryName.startsWith("ramls/") && entryName.endsWith(".json")) {
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

  private String getSchemaByName(String name, String okapiUrl) throws IOException {
    String schema = null;
    File jarFile = new File(getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
    JarFile jar = new JarFile(jarFile);
    List<JarEntry> entries = Collections.list(jar.entries());
    for (JarEntry entry : entries) {
      String entryName = entry.getName();
      if (entryName.startsWith("ramls/")) {
        String schemaName = entryName.substring(entryName.lastIndexOf("/") + 1);
        if (schemaName.equals(name)) {
          try {
            InputStream is = jar.getInputStream(entry);
            JsonNode schemaNode = ObjectMapperTool.getMapper().readValue(is, JsonNode.class);
            schema = replaceReferences(schemaNode.toString(), okapiUrl);
            is.close();
            break;
          } catch(IOException e) {
            log.info("{} is not a valid json file", entryName);
          }
        }
      }
    }
    jar.close();
    return schema;
  }

  private String replaceReferences(String schema, String okapiUrl) throws IOException {
    Matcher matcher = REF_MATCH_PATTERN.matcher(schema);
    StringBuffer sb = new StringBuffer(schema.length());
    while (matcher.find()) {
      String matchRef = matcher.group(1);
      String ref = matchRef.substring(matchRef.lastIndexOf("/") + 1);
      if (!matchRef.startsWith("#")) {
        matcher.appendReplacement(sb, Matcher.quoteReplacement("\"$ref\":\"" + okapiUrl + "/_/jsonSchema/" + ref + "\""));
      }
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

}
