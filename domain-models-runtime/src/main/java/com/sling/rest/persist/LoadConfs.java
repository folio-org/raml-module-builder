package com.sling.rest.persist;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.io.ByteStreams;

import io.vertx.core.json.JsonObject;

public class LoadConfs {

  public JsonObject loadConfig(String configFile) {

    boolean loadDefault = true;
    
    try {
      File file = new File(configFile);
      byte[] jsonData = null;
      if (file.isAbsolute()) {
        if (file.exists()) {
          jsonData = ByteStreams.toByteArray(new FileInputStream(file));
          loadDefault = false;
        }
      }
      if(loadDefault){
        jsonData = ByteStreams.toByteArray(getClass().getResourceAsStream(configFile));
      }
      return new JsonObject(new String(jsonData));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

}
