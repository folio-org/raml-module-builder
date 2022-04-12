package org.folio.rest.tools.utils;

import java.util.Collection;
import java.util.function.Function;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.folio.rest.RestVerticle;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;


public class LogUtil {

  private static final Logger log = LogManager.getLogger(LogUtil.class);

  public static void formatStatsLogMessage(String clientIP, String httpMethod, String httpVersion, int responseCode, long responseTime,
      long responseSize, String url, String queryParams, String message) {

    String message1 = new StringBuilder(injectDeploymentId()).append(clientIP).append(" ").append(httpMethod).append(" ").append(url).append(" ").append(queryParams)
        .append(" ").append(httpVersion).append(" ").append(responseCode).append(" ").append(responseSize).append(" ").append(responseTime)
        .append(" ").append(message).toString();

    log.info(message1);
  }

  public static void formatStatsLogMessage(String clientIP, String httpMethod, String httpVersion, int responseCode, long responseTime,
      long responseSize, String url, String queryParams, String message, String tenantId, String body) {

    String message1 = new StringBuilder(injectDeploymentId()).append(clientIP).append(" ").append(httpMethod).append(" ").append(url).append(" ").append(queryParams)
        .append(" ").append(httpVersion).append(" ").append(responseCode).append(" ").append(responseSize).append(" ").append(responseTime)
        .append(" tid=").append(tenantId).append(" ").append(message).append(" ").append(body).toString();

    log.info(message1);
  }

  /**
   * @return function.apply(t).toString(), or "null" if t is null or function.apply(t) returns null.
   */
  private static <T> String map(T t, Function<T,Object> function) {
    if (t == null) {
        return "null";
    }
    Object object = function.apply(t);
    if (object == null) {
      return "null";
    }
    return object.toString();
  }

  public static void formatStatsLogMessage(RoutingContext routingContext,
      long responseTime, String tenantId, String body) {

    if (routingContext == null) {
      log.info(injectDeploymentId() + responseTime + " tid=" + tenantId + " " + body);
      return;
    }
    HttpServerRequest request = routingContext.request();
    HttpServerResponse response = routingContext.response();
    formatStatsLogMessage(
        map(request, HttpServerRequest::remoteAddress),
        map(request, HttpServerRequest::method),
        map(request, HttpServerRequest::version),
        response == null ? -1 : response.getStatusCode(),
        responseTime,
        response == null ? -1 : response.bytesWritten(),
        map(request, HttpServerRequest::path),
        map(request, HttpServerRequest::query),
        map(response, HttpServerResponse::getStatusMessage),
        tenantId,
        body);
  }

  public static void formatLogMessage(String clazz, String function, String message) {
    log.info(new StringBuilder(injectDeploymentId()).append(clazz).append(" ").append(function).append(" ").append(message));
  }
  public static void formatErrorLogMessage(String clazz, String function, String message) {
    log.error(new StringBuilder(injectDeploymentId()).append(clazz).append(" ").append(function).append(" ").append(message));
  }

  /**
   * NOT SUPPORTED ANY MORE
   */
  public static void closeLogger() {

  }

  private static String injectDeploymentId(){
    if (log.isDebugEnabled() &&
        Vertx.currentContext() != null &&
        Vertx.currentContext().getInstanceCount() > 1 &&
        RestVerticle.getDeploymentId() != null) {
      return RestVerticle.getDeploymentId() + " ";
    }
    return "";
  }

  /**
   * Update the log level for all packages / a specific package / a specific class
   * @param packageName - pass "*" for all packages
   * @param level - see {@link Level}
   * @return - JsonObject with a list of updated loggers and their levels
   */
  public static JsonObject updateLogConfiguration(String packageName, String level){

    JsonObject updatedLoggers = new JsonObject();
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Collection<org.apache.logging.log4j.core.Logger> allLoggers = ctx.getLoggers();

    allLoggers.forEach( log -> {
      if(log != null && packageName != null && (log.getName().startsWith(packageName.replace("*", "")) || "*".equals(packageName)) ){
        if(log != null){
          log.setLevel(getLog4jLevel(level));
          updatedLoggers.put(log.getName(), log.getLevel().toString());
        }
      }
    });

    return updatedLoggers;
  }

  /**
   * Iterate over all loggers and return a json object with them and their log level
   * @return JsonObject
   */
  public static JsonObject getLogConfiguration(){

    JsonObject loggers = new JsonObject();
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Collection<org.apache.logging.log4j.core.Logger> allLoggers = ctx.getLoggers();
    allLoggers.forEach( log -> {
      if(log != null && log.getLevel() != null && log.getName() != null){
        loggers.put(log.getName(), log.getLevel().toString());
      }
    });

    return loggers;
  }

  public static void setLevelForRootLoggers(Level level){
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = config.getRootLogger();
    loggerConfig.setLevel(level);
    ctx.updateLoggers();
  }

  static Level getLog4jLevel(String level) {
    if (level == null) {
      return Level.INFO;
    }
    Level result = Level.toLevel(level, null);
    if (result != null) {
      return result;
    }
    // for backwards compatibility convert JUL levels to log4j levels
    switch (level.toUpperCase()) {
      case "SEVERE":  return Level.ERROR;
      case "WARNING": return Level.WARN;
      case "FINE":    return Level.DEBUG;
      case "FINER":
      case "FINEST":  return Level.TRACE;
      default:        return Level.INFO;
    }
  }
}
