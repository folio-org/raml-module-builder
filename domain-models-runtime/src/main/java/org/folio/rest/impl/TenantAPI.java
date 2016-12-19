package org.folio.rest.impl;

import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;
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
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.TenantTool;


/**
 * @author shale
 *
 */
public class TenantAPI implements org.folio.rest.jaxrs.resource.TenantResource {

  private static final String       CREATE_TENANT_TEMPLATE = "template_create_tenant.sql";
  private static final String       DELETE_TENANT_TEMPLATE = "template_delete_tenant.sql";
  private static final String       AUDIT_TENANT_TEMPLATE  = "template_audit.sql";
  private static final String       TEMPLATE_PLACEHOLDER   = "myuniversity";


  private static final Logger       log               = LoggerFactory.getLogger(TenantAPI.class);
  private final Messages            messages          = Messages.getInstance();


  @Validate
  @Override
  public void deleteTenant(Map<String, String> headers,
      Handler<AsyncResult<Response>> handlers, Context context) throws Exception {

    context.runOnContext(v -> {
      try {

        System.out.println("sending... deleteTenant");
        String tenantId = TenantTool.calculateTenantId( headers.get(OKAPI_HEADER_TENANT) );

        String sqlFile = IOUtils.toString(
          TenantAPI.class.getClassLoader().getResourceAsStream(DELETE_TENANT_TEMPLATE));

        final String sql2run = sqlFile.replaceAll(TEMPLATE_PLACEHOLDER, tenantId);

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
                handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse.withJsonOK(os)));
              } catch (Exception e) {
                log.error(e.getMessage(), e);
                handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse
                  .withPlainInternalServerError(e.getMessage())));
              }
            });
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse
          .withPlainInternalServerError(e.getMessage())));
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
        String tenantId = TenantTool.calculateTenantId( headers.get(OKAPI_HEADER_TENANT) );

        String sqlFile = IOUtils.toString(
          TenantAPI.class.getClassLoader().getResourceAsStream(CREATE_TENANT_TEMPLATE));

        String sql2run = sqlFile.replaceAll(TEMPLATE_PLACEHOLDER, tenantId);

        /* connect as user in postgres-conf.json file (super user) - so that all commands will be available */
        PostgresClient.getInstance(context.owner()).select(
          "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '"+tenantId+"');",
            reply -> {
              try {
                String res = "";
                if(reply.succeeded()){
                }
                handlers.handle(io.vertx.core.Future.succeededFuture(GetTenantResponse.withPlainOK("")));
              } catch (Exception e) {
                log.error(e.getMessage(), e);
                handlers.handle(io.vertx.core.Future.succeededFuture(GetTenantResponse
                  .withPlainInternalServerError(e.getMessage())));
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
      try {

        System.out.println("sending... postTenant");
        String tenantId = TenantTool.calculateTenantId( headers.get(OKAPI_HEADER_TENANT) );

        String sqlFile = IOUtils.toString(
          TenantAPI.class.getClassLoader().getResourceAsStream(CREATE_TENANT_TEMPLATE));

        final String sql2run = sqlFile.replaceAll(TEMPLATE_PLACEHOLDER, tenantId);

        /* is there an audit .sql file to load */
        InputStream audit = TenantAPI.class.getClassLoader().getResourceAsStream(AUDIT_TENANT_TEMPLATE);
        StringBuffer auditContent = new StringBuffer();
        if(audit != null){
          auditContent.append(IOUtils.toString(audit).replace(TEMPLATE_PLACEHOLDER, tenantId));
        }

        /* connect as user in postgres-conf.json file (super user) - so that all commands will be available */
        PostgresClient.getInstance(context.owner()).runSQLFile(sql2run, false,
            reply -> {
              try {
                StringBuffer res = new StringBuffer();
                if(reply.succeeded()){
                  res.append(new JsonArray(reply.result()).encodePrettily());
                  OutStream os = new OutStream();

                  if(audit != null){
                    PostgresClient.getInstance(context.owner()).runSQLFile(auditContent.toString(), false, reply2 -> {
                      if(reply2.succeeded()){
                        String auditRes = new JsonArray(reply2.result()).encodePrettily();
                        os.setData(res + auditRes);
                        handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.withJsonOK(os)));
                      }
                      else{
                        log.error(reply2.cause().getMessage(), reply2.cause());
                        handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse
                          .withPlainInternalServerError("Created tenant without auditing: " + reply2.cause().getMessage())));
                      }
                    });
                  }
                  else{
                    os.setData(res);
                    handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.withJsonOK(os)));
                  }
                }
                else{
                  log.error(reply.cause().getMessage(), reply.cause());
                  handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse
                    .withPlainInternalServerError(reply.cause().getMessage())));
                }
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
