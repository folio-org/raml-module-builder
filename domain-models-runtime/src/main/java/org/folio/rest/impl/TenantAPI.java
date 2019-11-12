package org.folio.rest.impl;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.resource.Tenant;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.ddlgen.Schema;
import org.folio.rest.persist.ddlgen.SchemaMaker;
import org.folio.rest.persist.ddlgen.TenantOperation;
import org.folio.rest.tools.ClientGenerator;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.TenantTool;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * @author shale
 *
 */
public class TenantAPI implements Tenant {

  public static final String       TABLE_JSON = "templates/db_scripts/schema.json";
  public static final String       DELETE_JSON = "templates/db_scripts/delete.json";

  private static final Logger       log               = LoggerFactory.getLogger(TenantAPI.class);

  PostgresClient postgresClient(Context context) {
    return PostgresClient.getInstance(context.owner());
  }

  @Validate
  @Override
  public void deleteTenant(Map<String, String> headers,
      Handler<AsyncResult<Response>> handlers, Context context) {

    context.runOnContext(v -> {
      try {

        String tenantId = TenantTool.calculateTenantId( headers.get(ClientGenerator.OKAPI_HEADER_TENANT) );
        log.info("sending... deleteTenant for " + tenantId);
        tenantExists(context, tenantId,
          h -> {
            boolean exists = false;
            if(h.succeeded()){
              exists = h.result();
              if(!exists){
                handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse.
                  respond400WithTextPlain("Tenant does not exist: " + tenantId)));
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
/*              InputStream is = TenantAPI.class.getClassLoader().getResourceAsStream(DELETE_JSON);
              if(is == null){
                log.info("No delete json to use for deleting tenant " + tenantId);
                handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse.withNoContent()));
                return;
              }
              sqlFile = IOUtils.toString(is);*/
              SchemaMaker sMaker = new SchemaMaker(tenantId, PostgresClient.getModuleName(), TenantOperation.DELETE, null, PomReader.INSTANCE.getRmbVersion());
              sqlFile = sMaker.generateDDL();

            } catch (Exception e1) {
              handlers.handle(io.vertx.core.Future.failedFuture(e1.getMessage()));
              log.error(e1.getMessage(), e1);
              return;
            }

            log.info("Attempting to run delete script for: " + tenantId);
            log.debug("GENERATED SCHEMA " + sqlFile);
            /* connect as user in postgres-conf.json file (super user) - so that all commands will be available */
            postgresClient(context).runSQLFile(sqlFile, true,
                reply -> {
                  try {
                    String res = "";
                    if(reply.succeeded()){
                      res = new JsonArray(reply.result()).encodePrettily();
                      if(reply.result().size() > 0){
                        log.error("Unable to run the following commands during tenant delete: ");
                        reply.result().forEach(System.out::println);
                        handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse.respond400WithTextPlain(res)));
                      }
                      else {
                        OutStream os = new OutStream();
                        os.setData(res);
                        handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse.respond204()));
                      }
                    }
                    else {
                      log.error(reply.cause().getMessage(), reply.cause());
                      handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse
                        .respond500WithTextPlain(reply.cause().getMessage())));
                    }
                  } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse
                      .respond500WithTextPlain(e.getMessage())));
                  }
                });
          });
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        handlers.handle(io.vertx.core.Future.succeededFuture(DeleteTenantResponse
          .respond500WithTextPlain(e.getMessage())));
      }
    });
  }

  void tenantExists(Context context, String tenantId, Handler<AsyncResult<Boolean>> handler){
    /* connect as user in postgres-conf.json file (super user) - so that all commands will be available */
    postgresClient(context).select(
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
      Context context)  {

    context.runOnContext(v -> {
      try {

        String tenantId = TenantTool.calculateTenantId( headers.get(ClientGenerator.OKAPI_HEADER_TENANT) );
        log.info("sending... postTenant for " + tenantId);

        tenantExists(context, tenantId, res -> {
          boolean exists = false;
          if(res.succeeded()){
            exists = res.result();
            handlers.handle(io.vertx.core.Future.succeededFuture(GetTenantResponse.respond200WithTextPlain(String.valueOf(
              exists))));
          }
          else{
            log.error(res.cause().getMessage(), res.cause());
            handlers.handle(io.vertx.core.Future.succeededFuture(GetTenantResponse
              .respond500WithTextPlain(res.cause().getMessage())));
          }
        });
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        handlers.handle(io.vertx.core.Future.succeededFuture(GetTenantResponse
          .respond500WithTextPlain(e.getMessage())));
      }
    });
  }


  @Validate
  @Override
  public void postTenant(TenantAttributes entity, Map<String, String> headers,
      Handler<AsyncResult<Response>> handlers, Context context)  {

    /**
     * http://host:port/tenant
     * Validation by rmb means the entity is either properly populated on is null
     * depending on whether this is an upgrade or a create tenant
     *
     * Modules that are not DB bound but are still RMB modules should override this API and do
     * any tenant bootstrapping they need
     */

    context.runOnContext(v -> {
      String tenantId = TenantTool.calculateTenantId(headers.get(ClientGenerator.OKAPI_HEADER_TENANT));
      log.info("sending... postTenant for " + tenantId);
      try {
        //body is optional so that the TenantAttributes
        if(entity != null){
          log.debug("upgrade from " + entity.getModuleFrom() + " to " + entity.getModuleTo());
        }

        tenantExists(context, tenantId,
          h -> {
            try {
              if (h.failed()) {
                handlers.handle(io.vertx.core.Future.failedFuture(h.cause().getMessage()));
                log.error(h.cause().getMessage(), h.cause());
                return;
              }
              final boolean tenantExists = h.result();
              InputStream tableInput = TenantAPI.class.getClassLoader().getResourceAsStream(
                TABLE_JSON);

              if(tableInput == null) {
                handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse
                  .respond204()));
                log.info("Could not find templates/db_scripts/schema.json , "
                    + " RMB will not run any scripts for " + tenantId);
                return;
              }

              TenantOperation op = TenantOperation.CREATE;
              String previousVersion = null;
              String newVersion = null;
              if (tenantExists) {
                op = TenantOperation.UPDATE;
                previousVersion = entity.getModuleFrom();
                newVersion = entity.getModuleTo();
              }

              SchemaMaker sMaker = new SchemaMaker(tenantId, PostgresClient.getModuleName(), op, previousVersion, newVersion);

              String tableInputStr = IOUtils.toString(tableInput, StandardCharsets.UTF_8);
              Schema schema = ObjectMapperTool.getMapper().readValue(tableInputStr, Schema.class);
              sMaker.setSchema(schema);

              String sqlFile = sMaker.generateDDL();
              log.debug("GENERATED SCHEMA " + sqlFile);
              postgresClient(context).runSQLFile(sqlFile, true,
                reply -> {
                  try {
                    if (reply.failed()) {
                      log.error(reply.cause().getMessage(), reply.cause());
                      handlers.handle(Future.succeededFuture(PostTenantResponse.
                        respond500WithTextPlain(reply.cause().getMessage())));
                      return;
                    }
                    boolean failuresExist = ! reply.result().isEmpty();
                    if (failuresExist) {
                      String res = new JsonArray(reply.result()).encodePrettily();
                      handlers.handle(Future.succeededFuture(
                        PostTenantResponse.respond400WithTextPlain(res)));
                      return;
                    }
                    String jsonListOfFailures = "[]";
                    PostTenantResponse postTenantResponse = tenantExists
                        ? PostTenantResponse.respond200WithApplicationJson(jsonListOfFailures)
                        : PostTenantResponse.respond201WithApplicationJson(jsonListOfFailures);
                    handlers.handle(Future.succeededFuture(postTenantResponse));
                  } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
                      respond500WithTextPlain(e.getMessage())));
                  }
                });
            } catch (Exception e) {
              log.error(e.getMessage(), e);
              handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
                respond500WithTextPlain(e.getMessage())));
            }
          });
      } catch (Exception e) {
        if(e.getMessage() != null){
          //if no db connection, the caught exception is a NPE which has a null for a message
          //which will print an NPE trace to the log which we dont want
          log.error(e.getMessage(), e);
        }
        handlers.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse.
          respond500WithTextPlain(e.getMessage())));
      }
    });
  }
}
