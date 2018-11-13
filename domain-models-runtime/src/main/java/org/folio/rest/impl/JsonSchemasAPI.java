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
import org.folio.rest.jaxrs.resource.JsonSchemas;
import org.folio.rest.tools.utils.ObjectMapperTool;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonSchemasAPI implements JsonSchemas {

  private static final Logger log = LoggerFactory.getLogger(JsonSchemasAPI.class);

  private static final Pattern REF_MATCH_PATTERN = Pattern.compile("\\\"\\$ref\\\"\\s*:\\s*\\\"(.*?)\\\"");

  private static final String OKAPI_URL_HEADER = "x-okapi-url";
  private static final String RAMLS_PATH = "ramls/";
  private static final String JSON_EXT = ".json";
  private static final String FORWARD_SLASH = "/";
  private static final String HASH_TAG = "#";

  @Validate
  @Override
  public void getJsonSchemas(
    String path,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext
  ) {
    vertxContext.runOnContext(v -> {
      log.info(path);
      try {
        if (path == null) {
          List<String> schemas = getSchemas();
          asyncResultHandler.handle(
            Future.succeededFuture(
              GetJsonSchemasResponse.respond200WithApplicationJson(schemas)
            )
          );
        } else {
          String okapiUrl = okapiHeaders.get(OKAPI_URL_HEADER);
          String schema = getSchemaByName(path, okapiUrl);
          if (schema != null) {
            asyncResultHandler.handle(
              Future.succeededFuture(
                GetJsonSchemasResponse.respond200WithApplicationSchemaJson(schema)
              )
            );
          } else {
            String notFoundMessage = "Schema " + path + " not found";
            asyncResultHandler.handle(
              Future.succeededFuture(
                GetJsonSchemasResponse.respond404WithTextPlain(notFoundMessage)
              )
            );
          }
        }
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        asyncResultHandler.handle(
          Future.succeededFuture(
            GetJsonSchemasResponse.respond500WithTextPlain(e.getMessage())
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
      if (entryName.startsWith(RAMLS_PATH) && entryName.endsWith(JSON_EXT)) {
        String schemaPath = entryName.substring(6);
        if(!schemaPath.contains(FORWARD_SLASH)) {
          String schemaName = schemaPath.substring(schemaPath.lastIndexOf(FORWARD_SLASH) + FORWARD_SLASH.length());
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

  private String getSchemaByName(String path, String okapiUrl) throws IOException {
    String schema = null;
    File jarFile = new File(getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
    JarFile jar = new JarFile(jarFile);
    List<JarEntry> entries = Collections.list(jar.entries());
    for (JarEntry entry : entries) {
      String entryName = entry.getName();
      if (entryName.startsWith(RAMLS_PATH) && entryName.endsWith(path)) {
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
    jar.close();
    return schema;
  }

  private String replaceReferences(String schema, String okapiUrl) throws IOException {
    Matcher matcher = REF_MATCH_PATTERN.matcher(schema);
    StringBuffer sb = new StringBuffer(schema.length());
    while (matcher.find()) {
      log.info(matcher.group(1));
      String matchRef = matcher.group(1);
      String path = matchRef.substring(matchRef.indexOf(RAMLS_PATH) + RAMLS_PATH.length());
      if (!matchRef.startsWith(HASH_TAG)) {
        matcher.appendReplacement(sb, Matcher.quoteReplacement("\"$ref\":\"" + okapiUrl + "/_/jsonSchemas?path=" + path + "\""));
      }
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

}
