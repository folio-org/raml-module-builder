package org.folio.rest.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.StringBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;
import java.util.jar.JarEntry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response;

import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.Raml;

import org.apache.commons.io.IOUtils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class RamlAPI implements Raml {

  private static final Logger log = LoggerFactory.getLogger(RamlAPI.class);

  private static final Pattern INCLUDE_MATCH_PATTERN = Pattern.compile("(?<=!include ).*");

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

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
    String okapiUrl = okapiHeaders.get("x-okapi-url");
    vertxContext.runOnContext(v -> {
      try {
        JsonNode ramlNode = getRamlByName(name, okapiUrl);
        if(ramlNode != null) {
          asyncResultHandler.handle(
            Future.succeededFuture(
              GetRamlByNameResponse.respond200WithApplicationJson(ramlNode)
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
            InputStream is = jar.getInputStream(entry);
            YAML_MAPPER.readValue(is, JsonNode.class);
            is.close();
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

  private JsonNode getRamlByName(String name, String okapiUrl) throws IOException {
    JsonNode ramlNode = null;
    File jarFile = new File(getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
    JarFile jar = new JarFile(jarFile);
    Enumeration<JarEntry> entries = jar.entries();
    while(entries.hasMoreElements()) {
      JarEntry entry = entries.nextElement();
      String entryName = entry.getName();
      if (entryName.startsWith("ramls/")) {
        String ramlName = entryName.substring(entryName.lastIndexOf("/") + 1);
        if (ramlName.equals(name)) {
          try {
            InputStream is = jar.getInputStream(entry);
            ramlNode = replaceReferences(IOUtils.toString(is, "UTF-8"), okapiUrl);
            is.close();
            break;
          } catch(IOException e) {
            log.info("{} is not a valid raml file", entryName);
          }
        }
      }
    }
    jar.close();
    return ramlNode;
  }

  private JsonNode replaceReferences(String raml, String okapiUrl) throws IOException {
    Matcher matcher = INCLUDE_MATCH_PATTERN.matcher(raml);
    StringBuffer sb = new StringBuffer(raml.length());
    while (matcher.find()) {
      String ref =  matcher.group(0).substring(matcher.group(0).lastIndexOf("/") + 1);
      if (ref.endsWith(".raml")) {
        matcher.appendReplacement(sb, Matcher.quoteReplacement(okapiUrl + "/_/raml/" + ref));
      } else {
        matcher.appendReplacement(sb, Matcher.quoteReplacement(okapiUrl + "/_/jsonSchema/" + ref));
      }
    }
    matcher.appendTail(sb);
    return YAML_MAPPER.readValue(sb.toString(), JsonNode.class);
  }

}
