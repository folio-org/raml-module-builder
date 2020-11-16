package org.folio.rest.persist;

import io.vertx.core.json.JsonObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.tools.utils.NetworkUtils;

import com.google.common.io.ByteStreams;

public final class LoadConfs {
  private static final Logger log = LogManager.getLogger(LoadConfs.class);

  private LoadConfs() throws IllegalAccessException {
    throw new IllegalAccessException("Cannot instantiate a utility class");
  }

  public static JsonObject loadConfig(String configFile) {
    boolean loadResource = true;

    try {
      if(NetworkUtils.isValidURL(configFile)){
        try {
          return new JsonObject(NetworkUtils.readURL(configFile));
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          return null;
        }
      }
      File file = new File(configFile);
      byte[] jsonData = null;
      if (file.isAbsolute()) {
        if (file.exists()) {
          jsonData = ByteStreams.toByteArray(new FileInputStream(file));
          loadResource = false;
          log.info("File has been loaded: " + configFile);
        } else {
          log.info("File does not exist: " + configFile);
        }
      }
      if (loadResource) {
          InputStream is = LoadConfs.class.getResourceAsStream(configFile);
          if (is == null) {
            log.info("Resource does not exist: " + configFile);
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
