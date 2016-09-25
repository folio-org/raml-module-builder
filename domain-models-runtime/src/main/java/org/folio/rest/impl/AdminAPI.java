package org.folio.rest.impl;

import java.io.BufferedReader;
import java.io.Reader;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;

import javax.mail.internet.MimeMultipart;
import javax.ws.rs.core.Response;

import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.AdminResource;
import org.folio.rest.tools.utils.LogUtil;
import org.folio.rest.tools.utils.OutStream;

public class AdminAPI implements AdminResource {

  private static final io.vertx.core.logging.Logger log               = LoggerFactory.getLogger(AdminAPI.class);

  private static final String DEFAULT_TEMP_DIR                        = System.getProperty("java.io.tmpdir");

  @Validate
  @Override
  public void putAdminLoglevel(String authorization, Level level, String javaPackage, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext)
      throws Exception {

    try {
      JsonObject updatedLoggers = LogUtil.updateLogConfiguration(javaPackage, level2level(level));
      OutStream os = new OutStream();
      os.setData(updatedLoggers);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.withJsonOK(os)));
    } catch (Exception e) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.withPlainInternalServerError("ERROR"
          + e.getMessage())));
      log.error(e.getMessage(), e);
    }

  }

  @Override
  public void getAdminLoglevel(String authorization, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    try {
      JsonObject loggers = LogUtil.getLogConfiguration();
      OutStream os = new OutStream();
      os.setData(loggers);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.withJsonOK(os)));
    } catch (Exception e) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.withPlainInternalServerError("ERROR"
          + e.getMessage())));
      log.error(e.getMessage(), e);
    }
  }

  private java.util.logging.Level level2level(Level level) {
    return java.util.logging.Level.parse(level.name());
  }

  @Override
  public void postAdminUpload(String authorization, PersistMethod persistMethod, String busAddress, String fileName, MimeMultipart entity,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    /**
     * THIS FUNCTION WILL NEVER BE CALLED - HANDLED IN THE RestVerticle class
     */
  }

  @Override
  public void putAdminCollstats(String authorization, Reader entity, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext)
      throws Exception {

    try {
      BufferedReader br = new BufferedReader(entity);
      String line;
      Buffer buffer = Buffer.buffer();
      while((line = br.readLine()) != null){
        buffer.appendString(line);
      }
      JsonObject job = new JsonObject(buffer.toString("UTF8"));
      MongoStatsPrinter.addCollection(job);
      OutStream os = new OutStream();
      os.setData(MongoStatsPrinter.getCollection());
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminCollstatsResponse.withJsonOK(os)));
    } catch (Exception e) {
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutAdminLoglevelResponse.withPlainInternalServerError("ERROR"
          + e.getMessage())));
        log.error(e.getMessage(), e);
    }

  }

}
