package org.folio.rest.persist;

import io.vertx.core.json.JsonObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.io.ByteStreams;

public final class LoadConfs {
  private static final Logger log = LogManager.getLogger(LoadConfs.class);

  private LoadConfs() throws IllegalAccessException {
    throw new UnsupportedOperationException("Cannot instantiate a utility class");
  }

  static byte[] loadFile(String configFile) throws IOException {
    File file = new File(configFile);
    if (! file.isAbsolute()) {
      log.info("File must be absolute, skipping: {}", configFile);
      return null;
    }
    if (! file.exists()) {
      log.info("File does not exist: {}", configFile);
      return null;
    }
    log.info("Loading File: {}", configFile);
    return ByteStreams.toByteArray(new FileInputStream(file));
  }

  static byte[] loadResource(String configFile) throws IOException {
    InputStream is = LoadConfs.class.getResourceAsStream(configFile);
    if (is == null) {
      log.info("Resource does not exist: {}", configFile);
      return null;
    }
    log.info("Resource has been loaded: {}", configFile);
    return ByteStreams.toByteArray(is);
  }

  public static JsonObject loadConfig(String configFile) {
    try {
      byte[] jsonData = loadFile(configFile);
      if (jsonData == null) {
        jsonData = loadResource(configFile);
      }
      if (jsonData != null) {
        return new JsonObject(new String(jsonData));
      }
    } catch (Exception e) {
      log.error(configFile, e);
    }
    return null;
  }
}
