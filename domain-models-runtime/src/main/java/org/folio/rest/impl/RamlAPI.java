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
import org.folio.rest.jaxrs.resource.Raml;
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

public class RamlAPI implements Raml {

  private static final Logger log = LoggerFactory.getLogger(RamlAPI.class);

  @Validate
  @Override
  public void getRaml(
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext
  ) {
    vertxContext.runOnContext(v -> {
      try {
        List<String> ramls = getRamls();
        asyncResultHandler.handle(
          Future.succeededFuture(
            GetRamlResponse.respond200WithApplicationJson(ramls)
          )
        );
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        asyncResultHandler.handle(
          Future.succeededFuture(
            GetRamlResponse.respond500WithTextPlain(e.getMessage())
          )
        );
      }
    });
  }

  @Validate
  @Override
  public void getRamlByName(
    String name,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext
  ) {
    vertxContext.runOnContext(v -> {
      try {
        String raml = getRamlByName(name);
        if(raml != null) {
          asyncResultHandler.handle(
            Future.succeededFuture(
              GetRamlByNameResponse.respond200WithTextPlain(raml)
            )
          );
        } else {
          String notFoundMessage = "RAML " + name + " not found";
          asyncResultHandler.handle(
            Future.succeededFuture(
              GetRamlByNameResponse.respond404WithTextPlain(notFoundMessage)
            )
          );
        }
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        asyncResultHandler.handle(
          Future.succeededFuture(
            GetRamlByNameResponse.respond500WithTextPlain(e.getMessage())
          )
        );
      }
    });
  }

  private List<String> getRamls() throws IOException {
    List<String> ramls = new ArrayList<>();
    File jarFile = new File(getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
    JarFile jar = new JarFile(jarFile);
    Enumeration<JarEntry> entries = jar.entries();
    while(entries.hasMoreElements()) {
      JarEntry entry = entries.nextElement();
      String entryName = entry.getName();
      if (entryName.startsWith("ramls/") && entryName.endsWith(".raml") && !entryName.startsWith("apidocs/")) {
        String ramlPath = entryName.substring(6);
        if(!ramlPath.contains("/")) {
          String ramlName = ramlPath.substring(ramlPath.lastIndexOf("/") + 1);
          try {
            // TODO: validate raml file
            ramls.add(ramlName);
          } catch(Exception e) {
            log.info("{} is not a valid raml file", entryName);
          }
        }
      }
    }
    jar.close();
    return ramls;
  }

  private String getRamlByName(String name) throws IOException {
    log.info("\n\n\n\n{}", name);
    String raml = null;
    File jarFile = new File(getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
    JarFile jar = new JarFile(jarFile);
    Enumeration<JarEntry> entries = jar.entries();
    while(entries.hasMoreElements()) {
      JarEntry entry = entries.nextElement();
      String entryName = entry.getName();
      log.info("  {}", entryName);
      if (entryName.startsWith("ramls/")) {
        String ramlName = entryName.substring(entryName.lastIndexOf("/") + 1);
        log.info("    {}", ramlName);
        if(ramlName.equals(name)) {
          log.info("** match ** ");
          try {
            // TODO: validate raml file
            InputStream is = jar.getInputStream(entry);
            raml = IOUtils.toString(is, "UTF-8");
            is.close();
            break;
          } catch(Exception e) {
            log.info("{} is not a valid raml file", entryName);
          }
        }
      }
    }
    jar.close();
    log.info("\n\n\n\n");
    return raml;
  }

}
