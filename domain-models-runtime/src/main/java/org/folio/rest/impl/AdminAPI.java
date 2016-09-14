package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;

import javax.mail.BodyPart;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMultipart;
import javax.ws.rs.core.Response;

import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.AdminResource;
import org.folio.rest.tools.utils.LogUtil;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.tools.utils.OutStream;


public class AdminAPI implements AdminResource {

  private static final io.vertx.core.logging.Logger log = LoggerFactory.getLogger(AdminAPI.class);

  private static final String DEFAULT_TEMP_DIR = System.getProperty("java.io.tmpdir");
  
  @Validate
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

  @Override
  public void postAdminUpload(MimeMultipart entity, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext)
      throws Exception {
    final int parts = entity.getCount();
    final int []i = new int[]{0};
    FileSystem fs = vertxContext.owner().fileSystem();
    for (; i[0] < parts; i[0]++) {
      BodyPart part = entity.getBodyPart(i[0]);
      Buffer buff = Buffer.buffer(NetworkUtils.object2Bytes(part.getContent()));
      final String filename = DEFAULT_TEMP_DIR + System.currentTimeMillis() + "_" + part.getFileName();
      fs.writeFile(filename, buff,      
        new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          try {
            if(result.succeeded()) {
              log.info("Uploaded file " + filename);
              if(i[0] == parts){
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostAdminUploadResponse.withNoContent(part.getFileName())));
              }
            } else {
              log.error("Failed uploading file " + DEFAULT_TEMP_DIR + "/" + System.currentTimeMillis() + "_" + part.getFileName()
                + ", error: " + result.cause().getMessage(), result.cause());
              if(i[0] == parts){
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostAdminUploadResponse.withPlainInternalServerError(
                  "ERROR uploading file")));
              }
            }
          } catch (MessagingException e) {
            e.printStackTrace();
          }
        }
      });
    }    
  }

  
}
