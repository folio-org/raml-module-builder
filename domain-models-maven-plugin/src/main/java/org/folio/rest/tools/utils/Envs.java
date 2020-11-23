package org.folio.rest.tools.utils;

import io.vertx.core.json.JsonObject;

import java.util.HashMap;
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

  @SuppressWarnings("squid:S3066")  // suppress "enum fields should not be publicly mutable"
  public static void setEnv(Map<String,String> env) {
    Envs.env = env;
  }

  public static String getEnv(Envs key){
    return env.get(key.name());
  }

  /**
   * Set DB_HOST, DB_PORT, DB_USERNAME, DB_PASSWORD and DB_DATABASE, unset all other variables.
   */
  public static void setEnv(String host, int port, String username, String password, String database) {
    Map<String,String> envs = new HashMap<>();
    envs.put("DB_HOST", host);
    envs.put("DB_PORT", port + "");
    envs.put("DB_USERNAME", username);
    envs.put("DB_PASSWORD", password);
    envs.put("DB_DATABASE", database);
    setEnv(envs);
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
