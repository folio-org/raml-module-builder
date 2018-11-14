package org.folio.rest.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.CodeSource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;

import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.Ramls;
import org.folio.rest.tools.utils.JarUtils;

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
  private static final String RAMLS_PATH = "ramls/";
  private static final String RAML_EXT = ".raml";
  private static final String FORWARD_SLASH = "/";

  private URL srcLocation;

  public RamlsAPI() throws IOException {
    super();
    init();
  }

  private void init() throws IOException {
    CodeSource src = getClass().getProtectionDomain().getCodeSource();
    srcLocation = src.getLocation();
    if (!srcLocation.toString().endsWith(".jar")) {
      srcLocation = JarUtils.archiveClasspath(srcLocation);
    }
  }

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

  private List<String> getRamls() throws IOException {
    List<String> ramls = new CopyOnWriteArrayList<>();
    try (JarFile jar = new JarFile(new File(srcLocation.getPath()))) {
      Collections.list(jar.entries()).parallelStream().forEach(entry -> {
        String entryName = entry.getName();
        if (entryName.startsWith(RAMLS_PATH) && entryName.endsWith(RAML_EXT)) {
          String ramlPath = entryName.substring(6);
          if (!ramlPath.contains(FORWARD_SLASH)) {
            String ramlName = ramlPath.substring(ramlPath.lastIndexOf(FORWARD_SLASH) + FORWARD_SLASH.length());
            try {
              ramls.add(ramlName);
            } catch(Exception e) {
              log.info("{} is not a valid raml file", entryName);
            }
          }
        }
      });
    }
    return ramls;
  }

  private String getRamlByPath(String path, String okapiUrl) throws IOException {
    String raml = null;
    try (JarFile jar = new JarFile(new File(srcLocation.getPath()))) {
      for (JarEntry entry : Collections.list(jar.entries())) {
        String entryName = entry.getName();
        if (entryName.equals(RAMLS_PATH + path)) {
          try {
            InputStream is = jar.getInputStream(entry);
            raml = replaceReferences(IOUtils.toString(is, StandardCharsets.UTF_8.name()), okapiUrl);
            is.close();
            break;
          } catch(IOException e) {
            log.info("{} is not a valid raml file", entryName);
          }
        }
      }
    }
    return raml;
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
