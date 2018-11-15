package org.folio.rest.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;

import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.Ramls;
import org.folio.rest.tools.GenerateRunner;

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
  private static final String JAR = "jar";

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
    URL ramlsListUrl = RamlsAPI.class.getClassLoader().getResource(RAMLS_PATH + GenerateRunner.RAML_LIST);
    try {
      if (ramlsListUrl.toURI().getScheme().equals(JAR)) {
        return IOUtils.readLines(RamlsAPI.class.getClassLoader().getResourceAsStream(RAMLS_PATH + GenerateRunner.RAML_LIST), StandardCharsets.UTF_8);
      } else {
        return Files.readAllLines(Paths.get(ramlsListUrl.toURI()), StandardCharsets.UTF_8);
      }
    } catch (URISyntaxException | IOException e) {
      return new ArrayList<>();
    }
  }

  private List<String> getRamls() throws IOException {
    return RAMLS;
  }

  private String getRamlByPath(String path, String okapiUrl) throws URISyntaxException, IOException {
    URL ramlUrl = getClass().getClassLoader().getResource(RAMLS_PATH + path);
    if (ramlUrl == null) {
      return null;
    }
    if (ramlUrl.toURI().getScheme().equals(JAR)) {
      InputStream is = getClass().getClassLoader().getResourceAsStream(RAMLS_PATH + path);
      return replaceReferences(IOUtils.toString(is, StandardCharsets.UTF_8.name()), okapiUrl);
    } else {
      return replaceReferences(IOUtils.toString(new FileInputStream(ramlUrl.getFile()), StandardCharsets.UTF_8.name()), okapiUrl);
    }
  }

  private String replaceReferences(String raml, String okapiUrl) {
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
