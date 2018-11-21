package org.folio.rest.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response;

import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.JsonSchemas;
import org.folio.rest.tools.GenerateRunner;
import org.folio.util.ResourceUtil;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class JsonSchemasAPI implements JsonSchemas {

  private static final Logger log = LoggerFactory.getLogger(JsonSchemasAPI.class);

  private static final Pattern REF_MATCH_PATTERN = Pattern.compile("\\\"\\$ref\\\"\\s*:\\s*\\\"(.*?)\\\"");

  private static final String OKAPI_URL_HEADER = "x-okapi-url";
  private static final String RAMLS_PATH = System.getProperty("raml_files", GenerateRunner.SOURCES_DEFAULT) + File.separator;
  private static final String HASH_TAG = "#";

  private static final List<String> JSON_SCHEMAS = getJsonSchemasList();

  @Validate
  @Override
  public void getJsonSchemas(
    String path,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext
  ) {
    vertxContext.runOnContext(v -> {
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
          String schema = getJsonSchemaByPath(path, okapiUrl);
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

  private static List<String> getJsonSchemasList() {
    try {
      return Arrays.asList(ResourceUtil.asString(RAMLS_PATH + GenerateRunner.JSON_SCHEMA_LIST).split("\\r?\\n"));
    } catch (IOException e) {
      log.warn("Unable to get JSON Schemas list!", e);
      return new ArrayList<>();
    }
  }

  private List<String> getSchemas() {
    return JSON_SCHEMAS;
  }

  private String getJsonSchemaByPath(String path, String okapiUrl) {
    try {
      return replaceReferences(ResourceUtil.asString(RAMLS_PATH + path), okapiUrl);
    } catch (IOException e) {
      return null;
    }
  }

  String replaceReferences(String schema, String okapiUrl) {
    Matcher matcher = REF_MATCH_PATTERN.matcher(schema);
    StringBuffer sb = new StringBuffer(schema.length());
    while (matcher.find()) {
      String path = matcher.group(1);
      if (!path.startsWith(HASH_TAG)) {
        if (path.contains(RAMLS_PATH)) {
          path = path.substring(path.lastIndexOf(RAMLS_PATH) + RAMLS_PATH.length());
        }
        matcher.appendReplacement(sb, Matcher.quoteReplacement("\"$ref\":\"" + okapiUrl + "/_/jsonSchemas?path=" + path + "\""));
      }
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

}
