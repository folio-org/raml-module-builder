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
  DB_CONNECTIONRELEASEDELAY,
  DB_EXPLAIN_QUERY_THRESHOLD;

  private static Map<String, String> env = System.getenv();

  static void setEnv(Map<String,String> env) {
    Envs.env = env;
  }

  public static String getEnv(Envs key){
    return env.get(key.name());
  }

  private static String configKey(Envs envs) {
    switch (envs) {
    case DB_QUERYTIMEOUT:            return "queryTimeout";
    case DB_MAXPOOLSIZE:             return "maxPoolSize";
    case DB_CONNECTIONRELEASEDELAY:  return "connectionReleaseDelay";
    case DB_EXPLAIN_QUERY_THRESHOLD: return envs.name();
    default:                         return envs.name().substring(3).toLowerCase();
    }
  }

  private static Object configValue(Envs envs, String value) {
    try {
      switch (envs) {
      case DB_PORT:
      case DB_QUERYTIMEOUT:
      case DB_MAXPOOLSIZE:
      case DB_CONNECTIONRELEASEDELAY:
        return Integer.parseInt(value);
      case DB_EXPLAIN_QUERY_THRESHOLD:
        return Long.parseLong(value);
      default:
        return value;
      }
    } catch (NumberFormatException e) {
      throw new NumberFormatException(envs.name() + ": " + e.getMessage());
    }
  }

  public static JsonObject allDBConfs() {
    JsonObject obj = new JsonObject();
    env.forEach((envKeyString, value) -> {
      if (! envKeyString.startsWith("DB_")) {
        return;
      }
      Envs envKey;
      try {
        envKey = Envs.valueOf(envKeyString);
      } catch (IllegalArgumentException e) {
        // skip unknown DB_ keys, for example DB_RUNNER_PORT.
        return;
      }
      String configKey = configKey(envKey);
      Object configValue = configValue(envKey, value);
      obj.put(configKey, configValue);
    });
    return obj;
  }

}
