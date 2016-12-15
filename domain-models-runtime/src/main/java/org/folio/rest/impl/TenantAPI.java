package org.folio.rest.impl;

import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.folio.rest.annotations.Validate;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.TenantTool;


/**
 * @author shale
 *
 */
public class TenantAPI implements org.folio.rest.jaxrs.resource.TenantResource {

  private static final Logger       log               = LoggerFactory.getLogger(TenantAPI.class);
  private final Messages            messages          = Messages.getInstance();


  @Validate
  @Override
  public void deleteTenant(Map<String, String> headers,
      Handler<AsyncResult<Response>> handlers, Context context) throws Exception {



  }

  @Validate
  @Override
  public void getTenant(Map<String, String> headers, Handler<AsyncResult<Response>> handlers,
      Context context) throws Exception {

  }

  @Validate
  @Override
  public void postTenant(Map<String, String> headers,
      Handler<AsyncResult<Response>> handlers, Context context) throws Exception {

    /**
    * http://host:port/tenant
    */
    context.runOnContext(v -> {
      try {

        System.out.println("sending... postTenant");
        String tenantId = TenantTool.calculateTenantId( headers.get(OKAPI_HEADER_TENANT) );

        String sqlFile = IOUtils.toString(
          TenantAPI.class.getClassLoader().getResourceAsStream("create_tenant.sql"));

        final String sql2run = sqlFile.replaceAll("myuniversity", tenantId);

        /* connect as user in postgres-conf.json file (super user) - so that all commands will be available */
        PostgresClient.getInstance(context.owner()).runSQLFile(sql2run, false,
            reply -> {
              try {
                String res = "";
                if(reply.succeeded()){
                  res = new JsonArray(reply.result()).encodePrettily();
                }
                OutStream os = new OutStream();
                os.setData(res);
                handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.withJsonOK(os)));
              } catch (Exception e) {
                log.error(e.getMessage(), e);
                handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse
                  .withPlainInternalServerError(e.getMessage())));
              }
            });
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse
          .withPlainInternalServerError(e.getMessage())));
      }
    });

  }

}
