package org.folio.rest.tools.utils;

import io.vertx.core.json.JsonObject;

import java.util.Map;

public enum Envs {
  DB_HOST,
  DB_PORT,
  DB_USERNAME,
  DB_PASSWORD,
  DB_DATABASE,
  DB_QUERYTIMEOUT,
  DB_CHARSET,
  DB_MAXPOOLSIZE;

  private static final String PORT = "port";
  private static final String TIMEOUT = "queryTimeout";
  private static final String MAXPOOL = "maxPoolSize";

  private static Map<String, String> env = System.getenv();

  static void setEnv(Map<String,String> env) {
    Envs.env = env;
  }

  public static String getEnv(Envs key){
    return env.get(key.name());
  }

  private static String toCamelCase(String key) {
    // two strings need camelCasing
    if (key.equalsIgnoreCase(TIMEOUT)) {
      return TIMEOUT;
    } else if (key.equalsIgnoreCase(MAXPOOL)) {
      return MAXPOOL;
    }
    return key;
  }

  public static JsonObject allDBConfs() {
    JsonObject obj = new JsonObject();
    env.forEach((key, value) -> {
      // also accept deprecated keys like "db.host" for "DB_HOST".
      if (key.startsWith("db.") || key.startsWith("DB_")) {
        key = toCamelCase(key.substring(3).toLowerCase());
        if (key.equals(PORT) || key.equals(TIMEOUT) || key.equals(MAXPOOL)) {
          obj.put(key, Integer.parseInt(value));
        } else {
          obj.put(key, value);
        }
      }
    });
    return obj;
  }

}
