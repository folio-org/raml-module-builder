package org.folio.rest.tools.utils;

import java.util.Enumeration;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.folio.rest.RestVerticle;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;


public class LogUtil {


  private static final Logger log = LogManager.getLogger(LogUtil.class);

  public static void formatStatsLogMessage(String clientIP, String httpMethod, String httpVersion, int ResponseCode, long responseTime,
      long responseSize, String url, String queryParams, String message) {

    String message1 = new StringBuilder(injectDeploymentId()).append(clientIP).append(" ").append(httpMethod).append(" ").append(url).append(" ").append(queryParams)
        .append(" ").append(httpVersion).append(" ").append(ResponseCode).append(" ").append(responseSize).append(" ").append(responseTime)
        .append(" ").append(message).toString();

    log.info(message1);
  }

  public static void formatStatsLogMessage(String clientIP, String httpMethod, String httpVersion, int ResponseCode, long responseTime,
      long responseSize, String url, String queryParams, String message, String tenantId, String body) {

    String message1 = new StringBuilder(injectDeploymentId()).append(clientIP).append(" ").append(httpMethod).append(" ").append(url).append(" ").append(queryParams)
        .append(" ").append(httpVersion).append(" ").append(ResponseCode).append(" ").append(responseSize).append(" ").append(responseTime)
        .append(" tid=").append(tenantId).append(" ").append(message).append(" ").append(body).toString();

    log.info(message1);
  }

  public static void formatLogMessage(String clazz, String funtion, String message) {
    log.info(new StringBuilder(injectDeploymentId()).append(clazz).append(" ").append(funtion).append(" ").append(message));
  }
  public static void formatErrorLogMessage(String clazz, String funtion, String message) {
    log.error(new StringBuilder(injectDeploymentId()).append(clazz).append(" ").append(funtion).append(" ").append(message));
  }

  /**
   * NOT SUPPORTED ANY MORE
   */
  public static void closeLogger() {

  }

  private static String injectDeploymentId(){
    if(LogManager.getLogger(RestVerticle.class).isDebugEnabled()){
      if(Vertx.currentContext() != null && Vertx.currentContext().getInstanceCount() > 1 &&
          RestVerticle.getDeploymentId() != null){
        return RestVerticle.getDeploymentId() + " ";
      }
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

    //log4j logs
    LoggerContext ctx = (LoggerContext) LogManager.getContext();
    Configuration cfg = ctx.getConfiguration();
    Map<String, LoggerConfig> loggerMap = cfg.getLoggers();
    //Enumeration<Logger> logger = LogManager.getLoggerRepository().getCurrentLoggers();
    loggerMap.forEach( (string, log) -> {
      if(log != null && packageName != null && (log.getName().startsWith(packageName.replace("*", "")) || "*".equals(packageName)) ){
        if(log != null){
          log.setLevel(Level.toLevel(level));
          updatedLoggers.put(log.getName(), log.getLevel().toString());
        }
      }
    });

    //JUL logs
    java.util.logging.LogManager manager = java.util.logging.LogManager.getLogManager();
    Enumeration<String> julLogs = manager.getLoggerNames();
    while (julLogs.hasMoreElements()) {
      String log = julLogs.nextElement();
      if(log != null && packageName != null && (log.startsWith(packageName.replace("*", "")) || "*".equals(packageName)) ){
        java.util.logging.Logger logger = manager.getLogger(log);
        if(logger != null){
          logger.setLevel(java.util.logging.Level.parse(level));
          updatedLoggers.put(logger.getName(), logger.getLevel().getName());
        }
      }
}
    return updatedLoggers;
  }

  /**
   * Iterate over all loggers and return a json object with them and their log level
   * @return JsonObject
   */
  public static JsonObject getLogConfiguration(){

    JsonObject loggers = new JsonObject();

    //log4j logs
    LoggerContext ctx = (LoggerContext) LogManager.getContext();
    Configuration cfg = ctx.getConfiguration();
    Map<String, LoggerConfig> logger = cfg.getLoggers();
    //Enumeration<Logger> logger = LogManager.getLoggerRepository().getCurrentLoggers();
    logger.forEach( (string, log) -> {
      if(log != null && log.getLevel() != null && log.getName() != null){
        loggers.put(log.getName(), log.getLevel().toString());
      }
    });

    //JUL logs
    java.util.logging.LogManager manager = java.util.logging.LogManager.getLogManager();
    Enumeration<String> loggerNames = manager.getLoggerNames();
    while (loggerNames.hasMoreElements()) {
      String log = loggerNames.nextElement();
      if(log != null){
        java.util.logging.Logger jul = manager.getLogger(log);
        if(jul != null && jul.getLevel() != null && jul.getName() != null){
          loggers.put(jul.getName(), jul.getLevel().getName());
        }
      }
    }

    return loggers;
  }

  public static void setLevelForLoggers(Level level){
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
    loggerConfig.setLevel(level);
    ctx.updateLoggers();
  }
}
