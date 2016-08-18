package com.sling.rest.resource.utils;

import java.util.logging.Level;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class LogUtil {

  private static final Logger log = LoggerFactory.getLogger("LogUtil");

  public static void formatStatsLogMessage(String clientIP, String httpMethod, String httpVersion, int ResponseCode, long responseTime,
      long responseSize, String url, String queryParams, String message) {

    String message1 = new StringBuilder(clientIP).append(" ").append(httpMethod).append(" ").append(url).append(" ").append(queryParams)
        .append(" ").append(httpVersion).append(" ").append(ResponseCode).append(" ").append(responseSize).append(" ").append(responseTime)
        .append(" ").append(message).toString();

    log.info(message1);
  }
  public static void formatLogMessage(String clazz, String funtion, String message) {
    log.info(new StringBuilder(clazz).append(" ").append(funtion).append(" ").append(message));
  }
  public static void formatErrorLogMessage(String clazz, String funtion, String message) {
    log.error(new StringBuilder(clazz).append(" ").append(funtion).append(" ").append(message));
  }
  public static void closeLogger() {
    LoggerFactory.removeLogger("LogUtil");
}
  
}
