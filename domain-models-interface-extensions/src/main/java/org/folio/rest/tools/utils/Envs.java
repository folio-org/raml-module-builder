package org.folio.rest.tools.utils;

import io.vertx.core.json.JsonObject;

import java.util.Iterator;
import java.util.Map;


public enum Envs {

  INSTANCE;

  public static final String DB_HOST = "db.host";
  public static final String DB_PORT = "db.port";
  public static final String DB_USER = "db.username";
  public static final String DB_PASSWORD = "db.password";
  public static final String DB_DATABASE = "db.database";
  public static final String DB_TIMEOUT = "db.queryTimeout";
  public static final String DB_CHARSET = "db.charset";

  private static Map<String, String> envs = null;

  static {
    envs = System.getenv();
  }

  public static String getEnv(String key){
    return envs.get(key);
  }

  public static JsonObject allDBConfs(){
    JsonObject obj = new JsonObject();
    Iterator<Map.Entry<String, String>> iter = envs.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, String> entry = iter.next();
      String key = entry.getKey();
      if(key.startsWith("db.")){
        String value = entry.getValue();
        if(key.equals(DB_PORT) || key.equals(DB_TIMEOUT)){
          obj.put(key.substring(3), Integer.valueOf(value).intValue());
        }
        else {
          obj.put(key.substring(3), value);
        }
      }
    }
    return obj;
  }

}
