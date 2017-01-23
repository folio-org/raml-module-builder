package org.folio.rest.persist;

import java.io.File;
import java.io.FileInputStream;

import com.google.common.io.ByteStreams;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.InputStream;

public final class LoadConfs {
  private static final Logger log = LoggerFactory.getLogger(LoadConfs.class);

  private LoadConfs() throws IllegalAccessException {
    throw new IllegalAccessException("Cannot instantiate a utility class");
  }

  public static JsonObject loadConfig(String configFile) {
    boolean loadResource = true;

    try {
      File file = new File(configFile);
      byte[] jsonData = null;
      if (file.isAbsolute()) {
        if (file.exists()) {
          jsonData = ByteStreams.toByteArray(new FileInputStream(file));
          loadResource = false;
          log.info("File has been loaded: " + configFile);
        } else {
          log.error("File does not exist: " + configFile);
        }
      }
      if (loadResource) {
          InputStream is = LoadConfs.class.getResourceAsStream(configFile);
          if (is == null) {
            log.error("Resource does not exist: " + configFile);
            return null;
          }
          log.info("Resource has been loaded: " + configFile);
          jsonData = ByteStreams.toByteArray(is);
      }
      return new JsonObject(new String(jsonData));
    } catch (Exception e) {
      log.error(configFile, e);
    }
    return null;
  }
}
