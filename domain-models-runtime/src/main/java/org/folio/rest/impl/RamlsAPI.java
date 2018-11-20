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
import org.folio.rest.jaxrs.resource.Ramls;
import org.folio.rest.tools.GenerateRunner;
import org.folio.util.ResourceUtil;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class RamlsAPI implements Ramls {

  private static final Logger log = LoggerFactory.getLogger(RamlsAPI.class);

  private static final Pattern INCLUDE_MATCH_PATTERN = Pattern.compile("(?<=!include ).*");

  private static final String OKAPI_URL_HEADER = "x-okapi-url";
  private static final String RAMLS_PATH = System.getProperty("raml_files", GenerateRunner.SOURCES_DEFAULT) + File.separator;
  private static final String RAML_EXT = ".raml";

  private static final List<String> RAMLS = getRamlsList();

  @Validate
  @Override
  public void getRamls(
    String path,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext
  ) {
    vertxContext.runOnContext(v -> {
      try {
        if (path == null) {
          List<String> ramls = getRamls();
          asyncResultHandler.handle(
            Future.succeededFuture(
              GetRamlsResponse.respond200WithApplicationJson(ramls)
            )
          );
        } else {
          String okapiUrl = okapiHeaders.get(OKAPI_URL_HEADER);
          String raml = getRamlByPath(path, okapiUrl);
          if (raml != null) {
            asyncResultHandler.handle(
              Future.succeededFuture(GetRamlsResponse.respond200WithApplicationRamlYaml(raml))
            );
          } else {
            String notFoundMessage = "RAML " + path + " not found";
            asyncResultHandler.handle(
              Future.succeededFuture(
                GetRamlsResponse.respond404WithTextPlain(notFoundMessage)
              )
            );
          }
        }
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        asyncResultHandler.handle(
          Future.succeededFuture(
            GetRamlsResponse.respond500WithTextPlain(e.getMessage())
          )
        );
      }
    });
  }

  private static List<String> getRamlsList() {
    try {
      return Arrays.asList(ResourceUtil.asString(RAMLS_PATH + GenerateRunner.RAML_LIST).split("\\r?\\n"));
    } catch (IOException e) {
      log.warn("Unable to get RAMLs list!", e);
      return new ArrayList<>();
    }
  }

  private List<String> getRamls() {
    return RAMLS;
  }

  private String getRamlByPath(String path, String okapiUrl) {
    try {
      return replaceReferences(ResourceUtil.asString(RAMLS_PATH + path), okapiUrl);
    } catch (IOException e) {
      return null;
    }
  }

  String replaceReferences(String raml, String okapiUrl) {
    Matcher matcher = INCLUDE_MATCH_PATTERN.matcher(raml);
    StringBuffer sb = new StringBuffer(raml.length());
    while (matcher.find()) {
      String path = matcher.group(0);
      if (path.contains(RAMLS_PATH)) {
        path = path.substring(path.lastIndexOf(RAMLS_PATH) + RAMLS_PATH.length());
      }
      if (path.endsWith(RAML_EXT)) {
        matcher.appendReplacement(sb, Matcher.quoteReplacement(okapiUrl + "/_/ramls?path=" + path));
      } else {
        matcher.appendReplacement(sb, Matcher.quoteReplacement(okapiUrl + "/_/jsonSchemas?path=" + path));
      }
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

}
