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
  DB_MAXPOOLSIZE,
  RMB_EXPLAIN_QUERY_THRESHOLD;

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
    return key.toLowerCase();
  }

  public static JsonObject allDBConfs() {
    JsonObject obj = new JsonObject();
    env.forEach((envKey, value) -> {
      if (! envKey.startsWith("DB_")) {
        return;
      }
      String key = toCamelCase(envKey.substring(3));
      if (key.equals(PORT) || key.equals(TIMEOUT) || key.equals(MAXPOOL)) {
        try {
          obj.put(key, Integer.parseInt(value));
        } catch (NumberFormatException e) {
          throw new NumberFormatException(envKey + ": " + e.getMessage());
        }
      } else {
        obj.put(key, value);
      }
    });
    return obj;
  }

}
