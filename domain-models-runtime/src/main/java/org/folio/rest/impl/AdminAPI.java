/**
 * AdminAPI
 * 
 * Sep 11, 2016
 *
 * Apache License Version 2.0
 */
package org.folio.rest.impl;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.logging.Logger;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;

import javax.ws.rs.core.Response;

import org.folio.rest.jaxrs.resource.AdminResource;
import org.folio.rest.jaxrs.resource.BooksResource.GetBooksResponse;
import org.folio.rest.tools.utils.LogUtil;
import org.folio.rest.tools.utils.OutStream;

/**
 * @author shale
 *
 */
public class AdminAPI implements AdminResource {

  private static final io.vertx.core.logging.Logger log = LoggerFactory.getLogger(AdminAPI.class);

  
  @Override
  public void putAdminLoglevel(Level level, String javaPackage, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext)
      throws Exception {
    
    try {
      JsonObject updatedLoggers = LogUtil.updateLogConfiguration(javaPackage, level2level(level));
      OutStream os = new OutStream();
      os.setData(updatedLoggers);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.withJsonOK(os)));
    } catch (Exception e) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.withPlainInternalServerError(
        "ERROR" + e.getMessage())));
      log.error(e.getMessage(), e);
    }

/*    HashSet<Logger> updatedLoggers = LogUtil.updateLogConfiguration(javaPackage, level2level(level));
    final StringBuffer sb = new StringBuffer();
    updatedLoggers.forEach( l -> {
      sb.append("[").append(l.getLevel().getName()).append(":").append(l.getName()).append("]\n");
    });
    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminLoglevelResponse.withPlainOK(sb.toString())));
    */
  }
  
  @Override
  public void getAdminLoglevel(Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    try {
      JsonObject loggers = LogUtil.getLogConfiguration();
      OutStream os = new OutStream();
      os.setData(loggers);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.withJsonOK(os)));
    } catch (Exception e) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.withPlainInternalServerError(
        "ERROR" + e.getMessage())));
      log.error(e.getMessage(), e);
    }
  }

  private java.util.logging.Level level2level(Level level){
    return java.util.logging.Level.parse(level.name());
  }


  
  
}
