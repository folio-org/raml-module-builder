package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.InputStream;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.folio.rest.annotations.Validate;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.ClientGenerator;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.TenantTool;


/**
 * @author shale
 *
 */
public class TenantAPI implements org.folio.rest.jaxrs.resource.TenantResource {

  public static final String       CREATE_TENANT_TEMPLATE = "template_create_tenant.sql";
  public static final String       DELETE_TENANT_TEMPLATE = "template_delete_tenant.sql";
  public static final String       AUDIT_TENANT_TEMPLATE  = "template_audit.sql";
  private static final String      TEMPLATE_TENANT_PLACEHOLDER   = "myuniversity";
  private static final String      TEMPLATE_MODULE_PLACEHOLDER   = "mymodule";


  private static final Logger       log               = LoggerFactory.getLogger(TenantAPI.class);
  private final Messages            messages          = Messages.getInstance();


  @Validate
  @Override
  public void deleteTenant(Map<String, String> headers,
      Handler<AsyncResult<Response>> handlers, Context context) throws Exception {

    context.runOnContext(v -> {
      try {

        System.out.println("sending... deleteTenant");
        String tenantId = TenantTool.calculateTenantId( headers.get(ClientGenerator.OKAPI_HEADER_TENANT) );

        tenantExists(context, tenantId,
          h -> {
            boolean exists = false;
            if(h.succeeded()){
              exists = h.result();
              if(!exists){
                handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
                  withPlainInternalServerError("Tenant does not exist: " + tenantId)));
                log.error("Can not delete. Tenant does not exist: " + tenantId);
                return;
              }
              else{
                log.info("Deleting tenant " + tenantId);
              }
            }
            else{
              handlers.handle(io.vertx.core.Future.failedFuture(h.cause().getMessage()));
              log.error(h.cause().getMessage(), h.cause());
              return;
            }

            String sqlFile = null;
            try {
              sqlFile = IOUtils.toString(
                TenantAPI.class.getClassLoader().getResourceAsStream(DELETE_TENANT_TEMPLATE));
            } catch (Exception e1) {
              handlers.handle(io.vertx.core.Future.failedFuture(e1.getMessage()));
              log.error(e1.getMessage(), e1);
              return;
            }

            String sql2run = sqlFile.replaceAll(TEMPLATE_TENANT_PLACEHOLDER, tenantId);
            sql2run = sql2run.replaceAll(TEMPLATE_MODULE_PLACEHOLDER, PostgresClient.getModuleName());
            /* connect as user in postgres-conf.json file (super user) - so that all commands will be available */
            PostgresClient.getInstance(context.owner()).runSQLFile(sql2run, false,
                reply -> {
                  try {
                    String res = "";
                    if(reply.succeeded()){
                      res = new JsonArray(reply.result()).encodePrettily();
                      if(reply.result().size() > 0){
                        handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse.withPlainBadRequest(res)));
                      }
                      else {
                        OutStream os = new OutStream();
                        os.setData(res);
                        handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse.withNoContent()));
                      }
                    }
                    else {
                      log.error(reply.cause().getMessage(), reply.cause());
                      handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse
                        .withPlainInternalServerError(reply.cause().getMessage())));
                    }
                  } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse
                      .withPlainInternalServerError(e.getMessage())));
                  }
                });
          });
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse
          .withPlainInternalServerError(e.getMessage())));
      }
    });
  }

  private void tenantExists(Context context, String tenantId, Handler<AsyncResult<Boolean>> handler){
    /* connect as user in postgres-conf.json file (super user) - so that all commands will be available */
    PostgresClient.getInstance(context.owner()).select(
      "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '"+ PostgresClient.convertToPsqlStandard(tenantId) +"');",
        reply -> {
          try {
            if(reply.succeeded()){
              handler.handle(io.vertx.core.Future.succeededFuture(reply.result().getResults().get(0).getBoolean(0)));
            }
            else {
              log.error(reply.cause().getMessage(), reply.cause());
              handler.handle(io.vertx.core.Future.failedFuture(reply.cause().getMessage()));
            }
          } catch (Exception e) {
            log.error(e.getMessage(), e);
            handler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
          }
    });
  }

  @Validate
  @Override
  public void getTenant(Map<String, String> headers, Handler<AsyncResult<Response>> handlers,
      Context context) throws Exception {

    context.runOnContext(v -> {
      try {

        System.out.println("sending... postTenant");
        String tenantId = TenantTool.calculateTenantId( headers.get(ClientGenerator.OKAPI_HEADER_TENANT) );

        tenantExists(context, tenantId, res -> {
          boolean exists = false;
          if(res.succeeded()){
            exists = res.result();
            handlers.handle(io.vertx.core.Future.succeededFuture(GetTenantResponse.withPlainOK(String.valueOf(
              exists))));
          }
          else{
            log.error(res.cause().getMessage(), res.cause());
            handlers.handle(io.vertx.core.Future.succeededFuture(GetTenantResponse
              .withPlainInternalServerError(res.cause().getMessage())));
          }
        });
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        handlers.handle(io.vertx.core.Future.succeededFuture(GetTenantResponse
          .withPlainInternalServerError(e.getMessage())));
      }
    });
  }

  @Validate
  @Override
  public void postTenant(Map<String, String> headers,
      Handler<AsyncResult<Response>> handlers, Context context) throws Exception {

    /**
     * http://host:port/tenant
     */
    context.runOnContext(v -> {
      System.out.println("sending... postTenant");
      String tenantId = TenantTool.calculateTenantId(headers.get(ClientGenerator.OKAPI_HEADER_TENANT));

      try {
        tenantExists(context, tenantId,
          h -> {
            try {
              boolean exists = false;
              if(h.succeeded()){
                exists = h.result();
                if(exists){
                  handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse
                    .withNoContent()));
                  log.error("Tenant already exists: " + tenantId);
                  return;
                }
                else{
                  log.info("adding tenant " + tenantId);
                }
              }
              else{
                handlers.handle(io.vertx.core.Future.failedFuture(h.cause().getMessage()));
                log.error(h.cause().getMessage(), h.cause());
                return;
              }
              InputStream stream = TenantAPI.class.getClassLoader().getResourceAsStream(
                CREATE_TENANT_TEMPLATE);
              if(stream == null){
                handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
                  withPlainInternalServerError("No Create tenant template found, can not create tenant")));
                log.error("No Create tenant template found, can not create tenant " + tenantId);
                return;
              }
              String sqlFile = IOUtils.toString(stream);

              String sql2run = sqlFile.replaceAll(TEMPLATE_TENANT_PLACEHOLDER, tenantId);
              sql2run = sql2run.replaceAll(TEMPLATE_MODULE_PLACEHOLDER, PostgresClient.getModuleName());

              /* is there an audit .sql file to load */
              InputStream audit = TenantAPI.class.getClassLoader().getResourceAsStream(
                AUDIT_TENANT_TEMPLATE);
              StringBuffer auditContent = new StringBuffer();
              if (audit != null) {
                auditContent.append(IOUtils.toString(audit).replace(TEMPLATE_TENANT_PLACEHOLDER, tenantId)
                  .replaceAll(TEMPLATE_MODULE_PLACEHOLDER, PostgresClient.getModuleName()));
              }
              /* connect as user in postgres-conf.json file (super user) - so that all commands will be available */
              PostgresClient.getInstance(context.owner()).runSQLFile(sql2run, false,
                reply -> {
                  try {
                    StringBuffer res = new StringBuffer();
                    if (reply.succeeded()) {
                      boolean failuresExist = false;
                      if(reply.result().size() > 0){
                        failuresExist = true;
                      }
                      res.append(new JsonArray(reply.result()).encodePrettily());
                      OutStream os = new OutStream();

                      if (audit != null && !failuresExist) {
                        PostgresClient.getInstance(context.owner()).runSQLFile(
                          auditContent.toString(),
                          false,
                          reply2 -> {
                            if (reply2.succeeded()) {
                              boolean failuresExistAudit = false;
                              if(reply2.result().size() > 0){
                                failuresExistAudit = true;
                              }
                              String auditRes = new JsonArray(reply2.result()).encodePrettily();
                              os.setData(res + auditRes);
                              if(failuresExistAudit){
                                handlers.handle(io.vertx.core.Future.succeededFuture(
                                  PostTenantResponse.withPlainBadRequest(auditRes)));
                              }
                              else{
                                os.setData(res);
                                handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
                                  withJsonCreated(os)));
                              }
                            } else {
                              log.error(reply2.cause().getMessage(), reply2.cause());
                              handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
                                withPlainInternalServerError("Created tenant without auditing: "
                                  + reply2.cause().getMessage())));
                            }
                          });
                      } else {
                        if(failuresExist){
                          handlers.handle(io.vertx.core.Future.succeededFuture(
                            PostTenantResponse.withPlainBadRequest(res.toString())));
                        }
                        else{
                          os.setData(res);
                          handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.withJsonCreated(os)));
                        }
                      }
                    } else {
                      log.error(reply.cause().getMessage(), reply.cause());
                      handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
                        withPlainInternalServerError(reply.cause().getMessage())));
                    }
                  } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
                      withPlainInternalServerError(e.getMessage())));
                  }
                });
            } catch (Exception e) {
              log.error(e.getMessage(), e);
              handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
                withPlainInternalServerError(e.getMessage())));
            }
          });
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    });
  }

}
