package org.folio.rest.impl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.JsonSchemas;
import org.folio.rest.resource.DomainModelConsts;
import org.folio.util.ResourceUtil;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public class JsonSchemasAPI implements JsonSchemas {

  private static final Logger log = LogManager.getLogger(JsonSchemasAPI.class);

  private static final Pattern REF_MATCH_PATTERN = Pattern.compile("\\\"\\$ref\\\"\\s*:\\s*\\\"(.*?)\\\"");

  /** resource path (jar, classes), not a file system path */
  private static final String RAMLS_PATH =
    (System.getProperty("raml_files", DomainModelConsts.SOURCES_DEFAULT) + '/').replace('\\', '/');
  private static final String HASH_TAG = "#";

  private static final List<String> JSON_SCHEMAS = getJsonSchemasList(RAMLS_PATH + DomainModelConsts.JSON_SCHEMA_LIST);

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
          String okapiUrl = okapiHeaders.get(XOkapiHeaders.URL);
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

  private static List<String> getJsonSchemasList(final String path) {
    try {
      return Arrays.asList(ResourceUtil.asString(path).split("\\r?\\n"));
    } catch (UncheckedIOException e) {
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
    } catch (IOException | UncheckedIOException e) {
      log.warn(e.getMessage());
      return null;
    }
  }

  static String replaceReferences(String schema, String okapiUrl) throws UnsupportedEncodingException {
    Matcher matcher = REF_MATCH_PATTERN.matcher(schema);
    StringBuffer sb = new StringBuffer(schema.length());
    while (matcher.find()) {
      String path = matcher.group(1);
      if (!path.startsWith(HASH_TAG)) {
        // The URI contains unix or windows path delimiter depending on which operating
        // system the jar was built.
        // Convert windows path: "file:C:%5CUsers%5Citsme%5Cmod-users%5Ctarget%5Cclasses%5Cuserdata.json"
        path = URLDecoder.decode(path, "UTF-8").replace('\\', '/');
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
